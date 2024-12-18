use super::*;

/// 测试节点0停止后的领导者选举
#[tokio::test]
async fn test_leader_stop_election() -> Result<()> {
    // 创建一个新的集群，并将节点0设置为领导者
    let cluster = RaftCluster::new_with_leader(0).await?;

    // 等待集群稳定
    time::sleep(Duration::from_millis(100)).await;

    // 验证节点0是领导者
    let (current_leader, term) = cluster.get_leader().ok_or_else(|| anyhow::anyhow!("没有领导者"))?;
    assert_eq!(current_leader, 0, "初始领导者应该是节点0");
    debug!("初始领导者: id={}, term={}", current_leader, term);

    // 获取节点0的共识模块并设置end_thread为true
    {
        let cm = cluster.get_cm(0)?;
        let mut cm = cm.lock().unwrap();
        cm.state = CMState::Dead;
        cm.end_thread = true;
    }
    debug!("已设置节点0的end_thread为true");

    // 等待，让新的领导者选举完成
    time::sleep(Duration::from_secs(6)).await;

    // 检查是否有新的领导者被选出
    if let Some((new_leader, new_term)) = cluster.get_leader() {
        // 验证新领导者不是节点0
        assert_ne!(new_leader, 0, "新领导者不应该是节点0");
        // assert!(new_term > term, "新任期应该大于旧任期");
        // 验证新领导者是节点1-4之间的一个
        assert!(new_leader >= 1 && new_leader <= 4, "新领导者应该是节点1-4之间的一个");
        debug!("选出新的领导者: id={}, term={}", new_leader, new_term);
        Ok(())
    } else {
        Err(anyhow::anyhow!("没有选出新的领导者"))
    }

}
