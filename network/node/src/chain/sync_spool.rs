use std::sync::Arc;

use rpc::{Rpc, RpcError};
use store::Store;
use tape_api::compute::SYNC_SPOOL_CU;
use tape_api::instruction::build_sync_spool_ix;
use tape_core::spooler::GroupIndex;
use tape_core::types::{EpochNumber, SpoolIndex};
use tape_crypto::tx::Txid;
use tape_protocol::Api;

use crate::context::NodeContext;

pub async fn submit_sync_spool<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    spool: SpoolIndex,
) -> Result<Txid, RpcError> {
    submit_sync_spools(ctx, epoch, &[spool]).await
}

/// Submit one transaction that syncs several spools at once.
pub async fn submit_sync_spools<Db: Store, Cluster: Api, Blockchain: Rpc>(
    ctx: &Arc<NodeContext<Db, Cluster, Blockchain>>,
    epoch: EpochNumber,
    spools: &[SpoolIndex],
) -> Result<Txid, RpcError> {
    let fee_payer = ctx.pubkey().into();
    let authority = ctx.pubkey().into();
    let node_address = ctx.node_address();

    let ixs = spools
        .iter()
        .map(|spool| {
            build_sync_spool_ix(
                fee_payer,
                authority,
                node_address,
                epoch,
                GroupIndex::containing(*spool),
                *spool,
            )
        })
        .collect::<Vec<_>>();

    let cu = SYNC_SPOOL_CU.saturating_mul(spools.len() as u32);

    ctx.rpc
        .send_instructions_with_compute_unit_limit(ctx.signer(), cu, ixs)
        .await
}

#[cfg(test)]
mod tests {
    use tape_core::system::EpochPhase;
    use tape_core::types::EpochNumber;

    use super::{submit_sync_spool, submit_sync_spools};
    use crate::harness::NodeHarness;
    use tape_core::spooler::GroupIndex;
    use tape_core::types::BitmapRead;

    const EPOCH: EpochNumber = EpochNumber(3);
    const NODE: usize = 7;

    #[tokio::test]
    async fn success() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Sync)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let spool = harness.owned_spools(NODE)[0];

        submit_sync_spool(&ctx, EPOCH, spool)
            .await
            .expect("submit sync spool");

        let authority = ctx.pubkey().address();
        let node = ctx.rpc.get_node(&authority).await.expect("fetch node");
        assert_eq!(node.latest_sync_epoch, EPOCH);
    }

    #[tokio::test]
    async fn success_batch() {
        let harness = NodeHarness::builder()
            .nodes(25)
            .epoch(EPOCH)
            .phase(EpochPhase::Sync)
            .build()
            .await
            .expect("build harness");
        let ctx = harness.ctx_for(NODE);
        let spools = harness.owned_spools(NODE);
        assert!(!spools.is_empty(), "node should own at least one spool");

        // One transaction for all of this node's spools.
        submit_sync_spools(&ctx, EPOCH, &spools)
            .await
            .expect("submit sync spool batch");

        let authority = ctx.pubkey().address();
        let node = ctx.rpc.get_node(&authority).await.expect("fetch node");
        assert_eq!(node.latest_sync_epoch, EPOCH);

        // Every owned spool's group must show this node's slice as synced.
        for spool in &spools {
            let group_id = GroupIndex::containing(*spool);
            let position = group_id.position_of(*spool).expect("spool position");
            let group = ctx.rpc.get_group(EPOCH, group_id).await.expect("fetch group");
            assert!(
                group.synced.is_set(position),
                "spool {spool} not marked synced after batch"
            );
        }
    }
}
