use super::*;

/// 测试节点0停止后的领导者选举, 单节点可能变为leader
#[tokio::test]
async fn test_leader_stop_election() -> Result<()> {
    // 创建一个新的集群，并将节点0设置为领导者
    let cluster = RaftCluster::new_with_leader(0, Vec::from([2])).await?;
    // 等待集群稳定
    time::sleep(Duration::from_millis(100)).await;

    // 验证节点0是领导者
    let (current_leader, term) = cluster.get_leader().ok_or_else(|| anyhow::anyhow!("没有领导者"))?;
    assert_eq!(current_leader, 0, "初始领导者应该是节点0");
    debug!("初始领导者: id={}, term={}", current_leader, term);
    debug_println(String::from("初始领导者为").add(current_leader.to_string().as_str())
        .add("\n任期为").add(term.to_string().as_str()));

    // 获取节点0的共识模块并设置end_thread为true
    {
        let cm = cluster.get_cm(0)?;
        let mut cm = cm.lock().unwrap();
        cm.state = CMState::Dead;
        cm.end_thread = true;
    }
    debug!("已设置节点0的end_thread为true");

    // 等待，让新的领导者选举完成
    time::sleep(Duration::from_secs(1)).await;

    // 检查是否有新的领导者被选出
    if let Some((new_leader, new_term)) = cluster.get_leader() {
        // 验证新领导者不是节点0
        assert_ne!(new_leader, 0, "新领导者不应该是节点0");
        // assert!(new_term > term, "新任期应该大于旧任期");
        // 验证新领导者是节点1-4之间的一个
        assert!(new_leader >= 1 && new_leader <= 4, "Accept!!!新领导者应该是节点1-4之间的一个");
        debug!("选出新的领导者: id={}, term={}", new_leader, new_term);
        debug_println(String::from("选出新的领导者: id=").add(new_leader.to_string().as_str())
            .add("\n任期为").add(new_term.to_string().as_str()));
        Ok(())
    } else {
        Err(anyhow::anyhow!("没有选出新的领导者"))
    }

}

/// 测试节点0停止后的领导者选举, 多节点可能变为leader
#[tokio::test]
async fn test_leader_stop_election_2() -> Result<()> {
    // 创建一个新的集群，并将节点0设置为领导者
    let cluster = RaftCluster::new_with_leader(0, Vec::from([2, 4])).await?;
    // 等待集群稳定
    time::sleep(Duration::from_millis(100)).await;

    // 验证节点0是领导者
    let (mut current_leader, term) = cluster.get_leader().ok_or_else(|| anyhow::anyhow!("没有领导者"))?;
    assert_eq!(current_leader, 0, "初始领导者应该是节点0");
    debug!("初始领导者: id={}, term={}", current_leader, term);

    // 获取节点0的共识模块并设置cm.state = CMState::Dead
    {
        let cm = cluster.get_cm(current_leader)?;
        let mut cm = cm.lock().unwrap();
        cm.state = CMState::Dead;
    }
    debug!("已设置节点0的cm.state = CMState::Dead");

    // 等待，让新的领导者选举完成
    time::sleep(Duration::from_secs(1)).await;

    // 检查是否有新的领导者被选出
    if let Some((new_leader, new_term)) = cluster.get_leader() {
        // 验证新领导者不是节点0
        assert_ne!(new_leader, 0, "新领导者不应该是节点0");
        // assert!(new_term > term, "新任期应该大于旧任期");
        // 验证新领导者是节点1-4之间的一个
        assert!(new_leader >= 1 && new_leader <= 4, "Accept!!!新领导者应该是节点1-4之间的一个");
        debug!("选出新的领导者: id={}, term={}", new_leader, new_term);
        debug_println(String::from("选出新的领导者: id=").add(new_leader.to_string().as_str())
            .add("\n任期为").add(new_term.to_string().as_str()));
        current_leader = new_leader;
        Ok(())
    } else {
        Err(anyhow::anyhow!("没有选出新的领导者"))
    }.expect("出错");

    {
        let cm = cluster.get_cm(current_leader)?;
        let mut cm = cm.lock().unwrap();
        cm.state = CMState::Dead;
        cm.end_thread = true;
    }
    debug!("已设置节点0的end_thread为true");

    // 等待，让新的领导者选举完成
    time::sleep(Duration::from_secs(1)).await;

    // 检查是否有新的领导者被选出
    if let Some((new_leader, new_term)) = cluster.get_leader() {
        // 验证新领导者不是节点0
        assert_ne!(new_leader, current_leader, "新领导者不应该是上一领导者节点节点");
        // assert!(new_term > term, "新任期应该大于旧任期");
        // 验证新领导者是节点1-4之间的一个
        assert!(new_leader >= 1 && new_leader <= 4, "Accept!!!新领导者应该是节点1-4之间的一个");
        debug!("选出新的领导者: id={}, term={}", new_leader, new_term);
        debug_println(String::from("选出新的领导者: id=").add(new_leader.to_string().as_str())
            .add("\n任期为").add(new_term.to_string().as_str()));
        Ok(())
    } else {
        Err(anyhow::anyhow!("没有选出新的领导者"))
    }
}

/// 测试死去的结点0复活后的状态
#[tokio::test]
async fn test_node0_revive() -> Result<()> {
    // 创建一个新的集群，并将节点0设置为领导者
    let cluster = RaftCluster::new_with_leader(0, Vec::from([2])).await?;
    // 等待集群稳定
    time::sleep(Duration::from_millis(100)).await;

    // 验证节点0是领导者
    let (current_leader, term) = cluster.get_leader().ok_or_else(|| anyhow::anyhow!("没有领导者"))?;
    assert_eq!(current_leader, 0, "初始领导者应该是节点0");
    debug!("初始领导者: id={}, term={}", current_leader, term);
    debug_println(String::from("初始领导者为").add(current_leader.to_string().as_str())
        .add("\n任期为").add(term.to_string().as_str()));

    // 获取节点0的共识模块并设置cm.state = CMState::Dead
    {
        let cm = cluster.get_cm(0)?;
        let mut cm = cm.lock().unwrap();
        cm.state = CMState::Dead;
    }
    debug!("已设置节点0的cm.state = CMState::Dead");

    // 等待，让新的领导者选举完成
    time::sleep(Duration::from_secs(1)).await;

    // 检查是否有新的领导者被选出
    if let Some((new_leader, new_term)) = cluster.get_leader() {
        // 验证新领导者不是节点0
        assert_ne!(new_leader, 0, "新领导者不应该是节点0");
        // assert!(new_term > term, "新任期应该大于旧任期");
        // 验证新领导者是节点1-4之间的一个
        assert!(new_leader >= 1 && new_leader <= 4, "Accept!!!新领导者应该是节点1-4之间的一个");
        debug!("选出新的领导者: id={}, term={}", new_leader, new_term);
        debug_println(String::from("选出新的领导者: id=").add(new_leader.to_string().as_str())
            .add("\n任期为").add(new_term.to_string().as_str()));
        Ok(())
    } else {
        Err(anyhow::anyhow!("没有选出新的领导者"))
    }.expect("failed");

    // 获取节点0的共识模块并设置cm.state = CMState::Dead
    {
        let cm = cluster.get_cm(0)?;
        let mut cm = cm.lock().unwrap();
        cm.state = CMState::Follower;
    }
    debug!("已设置节点0的end_thread为true");
    debug_println(String::from("节点0复活"));
    time::sleep(Duration::from_secs(1)).await;

    if let Some((_, tmp_leader_id, tmp_current_term, tmp_state, _)) = cluster.get_cm_info_by_id(0) {
        debug_println(String::from("节点0当前状态为\nleader_id=").add(tmp_leader_id.to_string().as_str())
            .add("\ntmp_current_term=").add(tmp_current_term.to_string().as_str())
            .add("\nstate=").add(tmp_state.to_string().as_str()));
        Ok(())
    }else {
        Err(anyhow::anyhow!("节点读取错误"))
    }
}

/// 测试死亡两个节点0 1前后以及复活0后, 其他节点同辈节点数量
#[tokio::test]
async fn test_node1_revive() -> Result<()> {
    // 创建一个新的集群，并将节点0设置为领导者
    let cluster = RaftCluster::new_with_leader(0, Vec::from([1, 2])).await?;
    // let mut tmp_debug_str = String::from("初始情况下情况:\n");
    // for i in 0..5 {
    //     if let Some((tmp_id, tmp_leader_id, tmp_term, tmp_state, tmp_peers)) = cluster.get_cm_info_by_id(i) {
    //         tmp_debug_str.push_str(&format!(
    //             "node_{}\nleader_id={}\nterm={}\nstate={}\npeers={}\n\n",
    //             tmp_id, tmp_leader_id, tmp_term, tmp_state, format!("{:?}", tmp_peers)
    //         ));
    //     }
    // }
    // debug_println(tmp_debug_str);
    // 等待集群稳定
    time::sleep(Duration::from_millis(100)).await;
    // let mut tmp_debug_str = String::from("初始情况下稳定后情况:\n");
    // for i in 0..5 {
    //     if let Some((tmp_id, tmp_leader_id, tmp_term, tmp_state, tmp_peers)) = cluster.get_cm_info_by_id(i) {
    //         tmp_debug_str.push_str(&format!(
    //             "node_{}\nleader_id={}\nterm={}\nstate={}\npeers={}\n\n",
    //             tmp_id, tmp_leader_id, tmp_term, tmp_state, format!("{:?}", tmp_peers)
    //         ));
    //     }
    // }
    // debug_println(tmp_debug_str);
    // 验证节点0是领导者
    let (current_leader, term) = cluster.get_leader().ok_or_else(|| anyhow::anyhow!("没有领导者"))?;
    debug_println(String::from("初始领导者为").add(current_leader.to_string().as_str())
        .add("\n任期为").add(term.to_string().as_str()));

    // 获取节点0 1的共识模块并设置cm.state = CMState::Dead
    for i in 0..2 {
        let cm = cluster.get_cm(i)?;
        let mut cm = cm.lock().unwrap();
        cm.state = CMState::Dead;
    }

    // 等待，让新的领导者选举完成, 信息同步完成
    time::sleep(Duration::from_secs(1)).await;

    let mut tmp_debug_str = String::from("节点0 1死亡状态下其他节点稳定后情况:\n");
    for i in 0..5 {
        if let Some((tmp_id, tmp_leader_id, tmp_term, tmp_state, tmp_peers)) = cluster.get_cm_info_by_id(i) {
            tmp_debug_str.push_str(&format!(
                "node_{}\nleader_id={}\nterm={}\nstate={}\npeers={}\n\n",
                tmp_id, tmp_leader_id, tmp_term, tmp_state, format!("{:?}", tmp_peers)
            ));
        }
    }
    debug_println(tmp_debug_str);

    // 获取节点0 1的共识模块并设置cm.state = CMState::Follower
    for i in 0..2 {
        let cm = cluster.get_cm(i)?;
        let mut cm = cm.lock().unwrap();
        cm.state = CMState::Follower;
    }

    // 等待信息同步完成
    time::sleep(Duration::from_secs(2)).await;

    let mut tmp_debug_str = String::from("节点0 1复活后其他节点稳定后情况:\n");
    for i in 0..5 {
        if let Some((tmp_id, tmp_leader_id, tmp_term, tmp_state, tmp_peers)) = cluster.get_cm_info_by_id(i) {
            tmp_debug_str.push_str(&format!(
                "node_{}\nleader_id={}\nterm={}\nstate={}\npeers={}\n\n",
                tmp_id, tmp_leader_id, tmp_term, tmp_state, format!("{:?}", tmp_peers)
            ));
        }
    }
    debug_println(tmp_debug_str);

    Ok(())

}