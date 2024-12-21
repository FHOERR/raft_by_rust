#[cfg(test)]
mod tests2;

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use std::ops::Add;
use tokio::{
    sync::mpsc,
    time,
};
use anyhow::Result;
use log::debug;
use rand::{Rng, rngs::OsRng};
use serde::{Deserialize, Serialize};
use regex::Regex;

/// 表示日志条目,包含任期号, 块高, leader_id, 要传递的信息
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct LogEntry {
    pub term: i32,
    pub height: i32,
    pub leader_id: i32,
    pub is_apply: bool,
    pub info: String,
}

impl LogEntry {
    pub fn new(term: i32, height:i32, leader_id: i32, info: String) -> LogEntry {
        LogEntry {
            term,
            height,
            leader_id,
            is_apply: false,
            info,
        }

    }

    /// 编码
    pub fn encode(&self) -> String {
        let string = String::from(format!(
            "LogEntry information: term= {}, height= {}, leader_id= {}, is_apply= {}, info= {}",
            self.term, self.height, self.leader_id, self.is_apply, self.info.clone()
        ));
        Self::encrypt(string)
    }

    /// 解码
    pub fn decode(string: String) -> Result<LogEntry> {
        let tmp_str = Self::decrypt(string);
        // debug_println(String::from("解码:\n当前文本为:\n").add(tmp_str.as_str()).add("/end"));
        let re = Regex::new(r"LogEntry information: term= (\d*), height= (\d*), leader_id= (\d*), is_apply= ([a-z]*), info= (\w*)").unwrap();

        if let Some(caps) = re.captures(&tmp_str) {
            let term = caps[1].parse::<i32>().unwrap();
            let height = caps[2].parse::<i32>().unwrap();
            let leader_id = caps[3].parse::<i32>().unwrap();
            let is_apply = caps[4].parse::<bool>().unwrap();
            let info: String = caps[5].to_string();
            // debug_println(String::from("解码成功"));
            Ok(LogEntry {
                term,
                height,
                leader_id,
                is_apply,
                info,
            })
        }else{
            Err(anyhow::Error::msg("解码错误"))
        }
    }

    /// 加密
    pub fn encrypt(string: String) -> String {
        // 不具体实现
        String::from(string)
    }

    /// 解密
    pub fn decrypt(string: String) -> String {
        String::from(string)
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct WSL {
    pub id: i32,
    pub logs: Vec<LogEntry>,
    pub local_path: String,
}

impl WSL {
    pub fn new(id: i32) -> WSL {
        WSL {
            id,
            logs: Vec::new(),
            local_path: String::from("WSL_data/node_").add(id.to_string().as_str()).add("_WSL"),
        }
    }

    /// 检查收到的区块string是否合法, 不需要decode后调用.
    /// 即String为加密状态
    pub fn check_block_by_string(&self, _string: String) -> bool {
        // 不进行具体实现了
        true
    }

    /// 检查区块结构体LogEntry是否合法
    pub fn check_block_by_entry(&self, log_entry: LogEntry) -> bool {
        self.check_block_by_string(log_entry.encode())
    }

    /// 获取最新日志
    pub fn get_newest(&self) -> LogEntry {
        if let Some(last_log) = self.logs.last() {
            last_log.clone()
        }else{
            LogEntry::new(0,0,0,String::from(""))
        }
    }

    /// 是否要打包交易成区块.
    /// 理论上需要检测交易数量, 达标了才打包
    pub fn check_should_package(&self) -> bool {
        // 不具体实现
        let tmp_rand = rand::random::<u32>() % (12u32);
        if tmp_rand > 0 {
            false
        }else{
            true
        }
    }

    /// 将交易打包成区块.
    /// 没写交易具体传递的细节, 所以不具体实现, 直接生成一个区块
    pub fn make_block(&self, term: i32, height: i32, leader_id: i32) -> LogEntry {
        LogEntry::new(term, height, leader_id, String::from("block information: term=")
            .add(term.to_string().as_str()).add(" height=").add(height.to_string().as_str())
            .add(" leader_id=").add(leader_id.to_string().as_str()))
    }
}

/// 表示节点状态的枚举类型
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
pub enum CMState {
    Follower,   // 跟随者状态
    Candidate,  // 候选人状态
    Leader,     // 领导者状态
    Dead,       // 已停止状态
}

impl std::fmt::Display for CMState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CMState::Follower => write!(f, "Follower"),
            CMState::Candidate => write!(f, "Candidate"),
            CMState::Leader => write!(f, "Leader"),
            CMState::Dead => write!(f, "Dead"),
        }
    }
}

/// 请求投票RPC的参数
#[derive(Debug, Deserialize, Serialize)]
pub struct RequestVoteArgs {
    pub term: i32,
    pub candidate_id: i32,
}

/// 请求投票RPC的响应
#[derive(Debug, Deserialize, Serialize)]
pub struct RequestVoteReply {
    pub term: i32,
    pub vote_granted: bool,
    pub leader_id: i32,
}

/// 心跳RPC的参数
#[derive(Debug, Deserialize, Serialize)]
pub struct AppendEntriesArgs {
    pub term: i32,
    pub leader_id: i32,
    pub prev_log_index: i32,
    pub prev_log_term: i32,
    pub entries: Vec<LogEntry>,
    pub leader_commit: i32,
    pub self_node_id: i32,
}

/// 心跳RPC的响应
#[derive(Debug, Deserialize, Serialize)]
pub struct AppendEntriesReply {
    pub term: i32,
    pub success: bool,
    pub leader_id: i32,
}

/// 消息RPC的参数
#[derive(Debug, Deserialize, Serialize)]
pub struct SendMsgArgs {
    pub id: i32,
    pub log_entry: String,
}

/// 请求投票RPC的响应
#[derive(Debug, Deserialize, Serialize)]
pub struct SendMsgReply {
    pub term: i32,
    pub height: i32,
    pub leader_id: i32,
    pub success: bool,
}

pub fn debug_println(string: String){
    println!("\
    /=====================```````````````````````````````````````=====================\\\n\n\n\
     {} \n\n\n\
    \\=====================.......................................=====================/\n", string);
}

/// 模拟共识算法中的网络连接
/// 1. 模拟RPC传输中的延迟
/// 2. 模拟不可靠的网络连接
pub struct RPCProxy {
    cm_list: Vec<Arc<Mutex<ConsensusModule>>>,
    rng: Arc<Mutex<OsRng>>,
}

impl RPCProxy {
    pub fn new(cm_list: Vec<Arc<Mutex<ConsensusModule>>>) -> Self {
        RPCProxy { 
            cm_list,
            rng: Arc::new(Mutex::new(OsRng)),
        }
    }

    /// 处理网络连接中投票请求
    pub async fn request_vote(&self, server_id: i32, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        // // 模拟不可靠的网络连接
        // if std::env::var("RAFT_UNRELIABLE_RPC").is_ok() {
        //     let dice = {
        //         let mut rng = self.rng.lock().unwrap();
        //         rng.gen_range(0..10)
        //     };
        //     if dice == 9 {
        //         // 模拟丢弃请求
        //         debug!("丢弃RequestVote");
        //         time::sleep(Duration::from_millis(75)).await;
        //         return Err(anyhow::anyhow!("RPC失败"));
        //     } else if dice == 8 {
        //         // 模拟RPC传输中的巨大延迟
        //         time::sleep(Duration::from_millis(50)).await;
        //     }
        // } else {
            // 模拟RPC传输中的延迟
            let delay = {
                let mut rng = self.rng.lock().unwrap();
                1 + rng.gen_range(0..3)
            };
            time::sleep(Duration::from_millis(delay)).await;
        // }
        
        // 获取目标节点的共识模块
        for (_, v) in self.cm_list.iter().enumerate() {
            let tmp = Arc::clone(v);
            let mut cm = tmp.lock().unwrap();
            if cm.id == server_id {
                return Ok(cm.request_vote(args));
            }
        }
        Err(anyhow::anyhow!("Invalid server ID"))
    }

    /// 处理网络连接中心跳信息
    pub async fn append_entries(&self, server_id: i32, args: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        // // 模拟不可靠的网络连接
        // if std::env::var("RAFT_UNRELIABLE_RPC").is_ok() {
        //     let dice = {
        //         let mut rng = self.rng.lock().unwrap();
        //         rng.gen_range(0..10)
        //     };
        //     if dice == 9 {
        //         // 模拟丢弃请求
        //         debug!("丢弃AppendEntries");
        //         time::sleep(Duration::from_millis(75)).await;
        //         return Err(anyhow::anyhow!("RPC失败"));
        //     } else if dice == 8 {
        //         // 模拟RPC传输中的巨大延迟
        //         time::sleep(Duration::from_millis(50)).await;
        //     }
        // } else {
            // 模拟RPC传输中的延迟
            let delay = {
                let mut rng = self.rng.lock().unwrap();
                1 + rng.gen_range(0..3)
            };
            time::sleep(Duration::from_millis(delay)).await;
        // }
        
        // 获取目标节点的共识模块
        for (_, v) in self.cm_list.iter().enumerate() {
            let tmp = Arc::clone(v);
            let mut cm = tmp.lock().unwrap();
            if cm.id == server_id {
                return Ok(cm.append_entries(args));
            }
        }
        Err(anyhow::anyhow!("Invalid server ID"))
    }

    /// 处理网络连接中区块消息
    pub async fn send_msg(&self, server_id: i32, send_msg_args: SendMsgArgs) -> Result<SendMsgReply> {
        // // 模拟不可靠的网络连接
        // if std::env::var("RAFT_UNRELIABLE_RPC").is_ok() {
        //     let dice = {
        //         let mut rng = self.rng.lock().unwrap();
        //         rng.gen_range(0..10)
        //     };
        //     if dice == 9 {
        //         // 模拟丢弃请求
        //         debug!("丢弃AppendEntries");
        //         time::sleep(Duration::from_millis(75)).await;
        //         return Err(anyhow::anyhow!("RPC失败"));
        //     } else if dice == 8 {
        //         // 模拟RPC传输中的巨大延迟
        //         time::sleep(Duration::from_millis(50)).await;
        //     }
        // } else {
        // 模拟RPC传输中的延迟
        let delay = {
            let mut rng = self.rng.lock().unwrap();
            1 + rng.gen_range(0..3)
        };
        time::sleep(Duration::from_millis(delay)).await;
        // }

        // 获取目标节点的共识模块
        for (_, v) in self.cm_list.iter().enumerate() {
            let tmp = Arc::clone(v);
            let mut cm = tmp.lock().unwrap();
            if cm.id == server_id {
                return Ok(cm.send_msg(send_msg_args.log_entry));
            }
        }
        Err(anyhow::anyhow!("Invalid server ID"))
    }

    /// 获取指定服务器的共识模块
    pub fn get_cm(&self, server_id: i32) -> Result<Arc<Mutex<ConsensusModule>>> {
        self.cm_list
            .get(server_id as usize)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Invalid server ID"))
    }
}

#[allow(dead_code)]
/// Server包装了ConsensusModule并管理RPC通信
pub struct Server {
    server_id: i32,
    peer_ids: Vec<i32>,
    cm: Arc<Mutex<ConsensusModule>>,
    rpc_proxy: Option<Arc<RPCProxy>>,
    // ready: mpsc::Receiver<()>,
    shutdown: bool,
    want_be_leader: bool, // 设置了一个是否想变为leader的选项, 用于控制领导者选举, 不至于全是领导者导致超时(其实也可以搞随机选举超时, 但比较麻烦)
}

impl Server {
    /// 创建新的服务器实例
    pub fn new(id: i32, peer_ids: Vec<i32>) -> Self {
        let cm = ConsensusModule::new(id, peer_ids.clone());
        Server {
            server_id: id,
            peer_ids,
            cm: Arc::new(Mutex::new(cm)),
            rpc_proxy: None,
            // ready,
            shutdown: false,
            want_be_leader: false,  // 默认为false
        }
    }

    /// 启动服务器
    pub async fn serve(&mut self) -> Result<()> {
        // // 等待启动信号
        // self.ready.recv().await;
        // // 没发信号
        // 启动选举检查任务
        let election_cm = Arc::clone(&self.cm);
        let election_rpc = self.rpc_proxy.clone();
        let server_id = self.server_id;

        // 选举处理
        tokio::spawn(async move {
            loop {
                let mut sleep_time = 100;
                // 检查是否需要开始选举
                let should_start_election = {
                    let mut cm = election_cm.lock().unwrap();
                    if cm.end_thread {
                        return
                    }
                    // if cm.election_reset_event.elapsed() >= cm.election_timeout_max {
                    //     debug_println(String::from("节点").add(cm.id.to_string().as_str()).add("发现leader心跳超时"));
                    // }
                    if cm.state == CMState::Leader && cm.self_last_heart_beat.elapsed() > cm.heart_beat_timeout_max {
                        // 如果本来是leader但是掉线了, 则暂时不参与选举, 并将状态转换为follower
                        cm.state = CMState::Follower;
                        cm.election_reset_event = Instant::now();
                        // 掉线后先通过心跳更新几轮
                        sleep_time = 400;
                        false
                    }else if cm.want_be_leader == false || cm.state == CMState::Dead {
                        // if cm.id == 2 {
                        //     debug_println(String::from("节点二尝试成为新leader失败1"));
                        // }
                        false
                    }else if cm.election_reset_event.elapsed() < cm.election_timeout() {
                        false
                    }else{
                        true
                    }
                };
                if should_start_election {
                    // 开始新的选举
                    let (current_term, peer_ids) = {
                        let mut cm = election_cm.lock().unwrap();
                        cm.start_election();
                        debug_println(String::from("节点").add(cm.id.to_string().as_str()).add("尝试成为新leader"));
                        (cm.current_term, cm.peer_ids.clone())
                    };

                    // 向所有对等节点发送请求投票RPC
                    for &peer_id in &peer_ids {
                        let args = RequestVoteArgs {
                            term: current_term,
                            candidate_id: server_id,
                        };

                        // 克隆需要的值用于async块
                        let rpc_proxy = election_rpc.clone().unwrap();
                        let election_cm = Arc::clone(&election_cm);

                        tokio::spawn(async move {
                            match rpc_proxy.request_vote(peer_id, args).await {
                                Ok(reply) => {
                                    let mut cm = election_cm.lock().unwrap();
                                    if reply.vote_granted {
                                        cm.votes_received += 1;
                                    } else if reply.term > current_term {
                                        cm.become_follower(reply.term, reply.leader_id);
                                    } else if reply.term >= current_term && reply.leader_id != cm.id {
                                        cm.become_follower(reply.term, reply.leader_id);
                                    }
                                }
                                Err(e) => debug!("RequestVote RPC失败: {}", e),
                            }
                        });
                    }
                    time::sleep(Duration::from_millis(40)).await;
                    {
                        let mut cm = election_cm.lock().unwrap();
                        if cm.votes_received * 2 > (cm.peer_ids.len() + 1) as i32 {
                            cm.become_leader();
                        }
                    }
                }

                // 等待一段时间再检查
                time::sleep(Duration::from_millis(sleep_time)).await;
            }
        });

        // 启动心跳发送任务
        let heartbeat_cm = Arc::clone(&self.cm);
        let heartbeat_rpc = self.rpc_proxy.clone();
        let server_id = self.server_id.clone();
        
        tokio::spawn(async move {
            loop {
                let mut leader_id = server_id.clone();
                // 检查是否需要发送心跳
                let (should_send_heartbeat, current_term, peer_ids) = {
                    let cm = heartbeat_cm.lock().unwrap();
                    if cm.end_thread {
                        return
                    }
                    if cm.state == CMState::Dead || cm.self_last_heart_beat.elapsed() < cm.heart_beat_timeout_min {
                        (false, 0, Vec::new())
                    } else {
                        leader_id = cm.leader_id.clone();
                        (true, cm.current_term.clone(), cm.peer_ids.clone())
                    }
                };

                if should_send_heartbeat {
                    // 向所有对等节点发送心跳
                    for &peer_id in &peer_ids {
                        let args = AppendEntriesArgs {
                            term: current_term,
                            leader_id: leader_id,
                            prev_log_index: 0,
                            prev_log_term: 0,
                            entries: Vec::new(),
                            leader_commit: 0,
                            self_node_id: server_id,
                        };

                        // 克隆需要的值用于async块
                        let rpc_proxy = heartbeat_rpc.clone().unwrap();
                        let heartbeat_cm = Arc::clone(&heartbeat_cm);

                        tokio::spawn(async move {
                            match rpc_proxy.append_entries(peer_id, args).await {
                                Ok(reply) => {
                                    let mut cm = heartbeat_cm.lock().unwrap();
                                    if !reply.success && reply.term > current_term {
                                        cm.become_follower(reply.term, reply.leader_id);
                                        // current_term = reply.term;
                                    } else if reply.term > cm.current_term && (cm.state == CMState::Leader || cm.state == CMState::Candidate) {
                                        cm.become_follower(reply.term, reply.leader_id);
                                    }
                                }
                                Err(e) => debug!("AppendEntries RPC失败: {}", e),
                            }
                        });
                    }

                    {
                        let mut cm = heartbeat_cm.lock().unwrap();
                        cm.self_last_heart_beat = Instant::now();
                        // if cm.id == 0 {
                        //     debug_println(String::from("node_0 heartbeat"));
                        // }
                        if cm.id == cm.leader_id {
                            cm.reset_election_timer();
                        }
                    }
                }

                // 等待一段时间再发送下一轮心跳
                time::sleep(Duration::from_millis(100)).await;
            }
        });

        // 定时处理peer id, 心跳过期就删除
        let del_cm = Arc::clone(&self.cm);
        tokio::spawn(async move {
            loop {
                let should_delete = {
                    let cm = del_cm.lock().unwrap();
                    if cm.end_thread {
                        return;
                    }
                    if cm.state == CMState::Dead {
                        false
                    }else{
                        true
                    }
                };

                if should_delete {
                    // 如果状态是Dead，删除节点
                    let mut cm = del_cm.lock().unwrap();
                    let mut del_vec = Vec::new();
                    for (i, &v) in cm.heart_beat_events.iter().enumerate() {
                        if v.elapsed() > cm.heart_beat_timeout_max {
                            del_vec.push(i);
                        }
                    }
                    for index in del_vec.iter().rev() {
                        cm.heart_beat_events.remove(*index);
                        cm.peer_ids.remove(*index);
                    }
                }
                time::sleep(Duration::from_millis(200)).await;
            }
        });

        // 消息发送
        let msg_cm = Arc::clone(&self.cm);
        let msg_rpc = self.rpc_proxy.clone();
        tokio::spawn(async move {
            loop {
                let (should_send_receive, is_leader) = {
                    let cm = msg_cm.lock().unwrap();
                    if cm.end_thread {
                        return;
                    }
                    if cm.state == CMState::Dead {
                        (false, cm.leader_id == cm.id)
                    } else {
                        (true, cm.leader_id == cm.id)
                    }
                };
                if should_send_receive && is_leader{
                    let mut peer_ids = Vec::new();
                    let mut tmp_block: LogEntry;
                    let mut node_id: i32;
                    {
                        let mut cm = msg_cm.lock().unwrap();
                        if !cm.log.check_should_package() {
                            continue;
                        }
                        peer_ids = cm.peer_ids.clone();
                        tmp_block = cm.log.make_block(cm.current_term, cm.log.get_newest().height + 1, cm.leader_id);
                        node_id = cm.id;
                        cm.apply_log_vote_received = 0;
                    }
                    let mut msg_string = tmp_block.encode();

                    for &peer_id in &peer_ids {
                        let args = SendMsgArgs {
                            id: node_id,
                            log_entry: msg_string.clone(),
                        };
                        let rpc_proxy = msg_rpc.clone().unwrap();
                        let send_cm = Arc::clone(&msg_cm);
                        tokio::spawn(async move {
                            match rpc_proxy.send_msg(peer_id, args).await {
                                Ok(reply) => {
                                    let mut cm = send_cm.lock().unwrap();
                                    if !reply.success && (reply.term > cm.current_term || reply.height > tmp_block.height) {
                                        cm.become_follower(reply.term, reply.leader_id);
                                        cm.last_applied = reply.height;
                                        cm.current_term = reply.term;
                                        cm.apply_log_vote_received = -1000000000;
                                        return
                                    }
                                    cm.apply_log_vote_received += 1;
                                }
                                Err(e) => {
                                    debug!("SendMsg RPC失败: {}", e)
                                }
                            }
                        });
                    }
                    time::sleep(Duration::from_millis(40)).await;
                    {
                        let mut cm = msg_cm.lock().unwrap();
                        if cm.apply_log_vote_received * 2 > (cm.peer_ids.len() + 1) as i32 {
                            tmp_block.is_apply = true;
                            msg_string = tmp_block.encode();
                            cm.last_applied = tmp_block.height;
                            cm.log.logs.push(tmp_block);
                            for &peer_id in &peer_ids {
                                let args = SendMsgArgs {
                                    id: node_id,
                                    log_entry: msg_string.clone(),
                                };
                                let rpc_proxy = msg_rpc.clone().unwrap();
                                tokio::spawn(async move {
                                    match rpc_proxy.send_msg(peer_id, args).await {
                                        Ok(_) => {
                                        }
                                        Err(e) => {
                                            debug!("SendMsg RPC失败: {}", e)
                                        }
                                    }
                                });
                            }
                        }
                    }
                }
                time::sleep(Duration::from_millis(10)).await;

            }
        });
        Ok(())
    }

    /// 关闭服务器
    pub async fn shutdown(&mut self) {
        self.shutdown = true;
        {
            let tmp = Arc::clone(&self.cm);
            let mut cm = tmp.lock().unwrap();
            cm.end_thread = true;
        }
    }

    pub fn set_rpc_proxy(&mut self, rpc_proxy: Arc<RPCProxy>) {
        self.rpc_proxy = Some(rpc_proxy);
    }
}

#[allow(dead_code)]
/// ConsensusModule实现了Raft共识的单个节点
pub struct ConsensusModule {
    id: i32,                                 // 节点ID
    peer_ids: Vec<i32>,                      // 其他节点的ID列表
    leader_id: i32,                          // leader id
    current_term: i32,                       // 当前任期
    voted_for: Option<i32>,                  // 在当前任期投票给谁
    log: WSL,                                // 日志
    apply_log_vote_received:i32,             // 当前发送的日志接受票数
    is_spv: bool,                            // 是否SPV节点
    state: CMState,                          // 当前状态
    election_reset_event: Instant,           // 上次重置选举计时器的时间
    election_timeout_min: Duration,          // 最小选举超时时间
    election_timeout_max: Duration,          // 最大选举超时时间
    self_last_heart_beat: Instant,           // 自己上次心跳时间
    heart_beat_events: Vec<Instant>,         // 上次收到其他节点的心跳时间
    heart_beat_timeout_min: Duration,        // 最小心跳超时时间
    heart_beat_timeout_max: Duration,        // 最大心跳超时时间
    votes_received: i32,                     // 收到的投票数
    commit_index: i32,                       // 已提交的最高日志索引
    last_applied: i32,                       // 已应用到状态机的最高日志索引
    end_thread: bool,                        // 结束tag
    want_be_leader: bool,                    // 想要变成leader
}

impl ConsensusModule {
    /// 创建新的共识模块实例
    pub fn new(id: i32, peer_ids: Vec<i32>) -> Self {
        let mut tmp_self = ConsensusModule {
            id,
            peer_ids,
            leader_id: 0,
            current_term: 0,
            voted_for: None,
            log: WSL::new(id),
            apply_log_vote_received: 0,
            is_spv: false,
            state: CMState::Follower,
            election_reset_event: Instant::now(),
            election_timeout_min: Duration::from_millis(200),
            election_timeout_max: Duration::from_millis(400),
            self_last_heart_beat: Instant::now(),
            heart_beat_events: Vec::new(),
            heart_beat_timeout_min: Duration::from_millis(40),
            heart_beat_timeout_max: Duration::from_millis(100),
            votes_received: 0,
            commit_index: -1,
            last_applied: -1,
            end_thread: false,
            want_be_leader: false,
        };
        tmp_self.init_peer_ids_heart();
        tmp_self
    }

    /// 返回选举超时时间
    ///
    /// 随机超时机制, 减小同时选举的可能
    fn election_timeout(&self) -> Duration {
        let base = self.election_timeout_min.as_millis(); // 基础超时时间
        let rand = rand::random::<u128>() % (self.election_timeout_max - self.election_timeout_min).as_millis();
        Duration::from_millis((base + rand) as u64)
    }

    // 初始化心跳信息
    fn init_peer_ids_heart(&mut self) {
        for (_, _) in self.peer_ids.iter().enumerate() {
            self.heart_beat_events.push(Instant::now());
        }
    }

    /// 重置选举超时计时器
    fn reset_election_timer(&mut self) {
        self.election_reset_event = Instant::now();
    }

    /// 转变为跟随者状态
    fn become_follower(&mut self, term: i32, leader_id: i32) {
        debug!("[{}] 变为Follower, term={}", self.id, term);
        self.state = CMState::Follower;
        self.leader_id = leader_id;
        self.current_term = term;
        self.voted_for = None;
        self.reset_election_timer();
    }

    /// 变为领导者
    fn become_leader(&mut self) {
        debug!("[{}] 变为Leader, term={}", self.id, self.current_term);
        self.state = CMState::Leader;
        self.leader_id = self.id;

        // 初始化领导者状态
        // TODO();

        self.reset_election_timer();
    }

    /// 处理请求投票RPC
    pub fn request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        debug!("[{}] 收到投票请求: candidate={}, term={}, local_term={}, voted_for={:?}", 
            self.id, args.candidate_id, args.term, self.current_term, self.voted_for);

        // 如果节点已死，不参与投票
        if self.state == CMState::Dead {
            debug!("[{}] 节点已停止，不参与投票", self.id);
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
                leader_id: args.candidate_id,
            };
        }

        if args.term < self.current_term {
            debug!("[{}] 拒绝投票: 任期过低 local_term={}, term={}", self.id, self.current_term, args.term);
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
                leader_id: self.leader_id,
            };
        }

        if args.term > self.current_term {
            debug!("[{}] 发现更高的任期: local={}, remote={}", 
                self.id, self.current_term, args.term);
            self.become_follower(args.term, args.candidate_id);
        }

        // 如果还没有投票，或者已经投给了这个候选人
        if self.voted_for.is_none() || self.voted_for == Some(args.candidate_id) {
            debug!("[{}] 同意投票给节点{}, term={}", self.id, args.candidate_id, args.term);
            self.voted_for = Some(args.candidate_id);
            self.leader_id = args.candidate_id;
            self.current_term = args.term;
            self.reset_election_timer();
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: true,
                leader_id: args.candidate_id,
            };
        }

        debug!("[{}] 拒绝投票: 已经投给了节点{:?}, term={}", self.id, self.voted_for, args.term);
        RequestVoteReply {
            term: self.current_term,
            vote_granted: false,
            leader_id: self.leader_id,
        }
    }

    /// 处理心跳信息
    pub fn append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if self.state == CMState::Dead {
            return AppendEntriesReply {
                term: 0,
                success: false,
                leader_id: args.leader_id,
            };
        }

        // 需要添加更新心跳时间, 加入节点的功能
        let mut tmp_index = self.peer_ids.len();
        if !self.peer_ids.is_empty() {
            for (i,&v) in self.peer_ids.iter().enumerate() {
                if v == args.self_node_id {
                    tmp_index = i;
                    break;
                }
            }
        }
        // debug_println(String::from("tmp_index = ").add(tmp_index.to_string().as_str())
        //     .add("\npeer_ids.len = ").add(self.peer_ids.len().to_string().as_str())
        //     .add("\nheart_beat_events = ").add(self.heart_beat_events.len().to_string().as_str()));
        if tmp_index >= self.peer_ids.len() {
            // 新建节点
            self.peer_ids.push(args.self_node_id);
            self.heart_beat_events.push(Instant::now());
        }else{
            // 更新心跳
            self.heart_beat_events[tmp_index] = Instant::now();
        }

        // 如果leader的任期小于当前任期，拒绝请求
        if args.term < self.current_term {
            debug!("[{}] 拒绝追加日志: leader_term({}) < current_term({})", self.id, args.term, self.current_term);
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                leader_id: self.leader_id,
            };
        }

        // 如果leader的任期大于当前任期，更新当前任期并转为follower
        if args.term > self.current_term {
            debug!("[{}] 发现更大的任期: term={}", self.id, args.term);
            self.become_follower(args.term, args.leader_id);
        }

        if args.self_node_id == self.leader_id{
            // 重置选举超时计时器
            self.reset_election_timer();
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
            leader_id: args.leader_id,
        }
    }

    /// 处理区块消息
    pub fn send_msg(&mut self, msg_string: String) -> SendMsgReply {
        let log_entry = LogEntry::decode(msg_string.clone()).expect("decode失败");
        if self.state == CMState::Dead {
            return SendMsgReply {
                term: self.current_term,
                height: self.last_applied,
                leader_id: log_entry.leader_id,
                success: false,
            };
        }
        if log_entry.term < self.current_term || log_entry.height <= self.last_applied {
            return SendMsgReply {
                term: self.current_term,
                height: self.last_applied,
                leader_id: self.leader_id,
                success: false,
            }
        }
        // 如果是应用的消息则应用, 如果是投票消息则投票
        if log_entry.is_apply {
            self.current_term = log_entry.term;
            self.last_applied = log_entry.height;
            self.leader_id = log_entry.leader_id;
            self.log.logs.push(log_entry.clone());
        }else if !self.log.check_block_by_string(msg_string) {
            return SendMsgReply {
                term: self.current_term,
                height: self.last_applied,
                leader_id: self.leader_id,
                success: false,
            }
        }

        SendMsgReply {
            term: self.current_term,
            height: self.last_applied,
            leader_id: log_entry.leader_id,
            success: true,
        }
    }

    /// 转变为候选人状态并开始选举
    fn start_election(&mut self) {
        if self.state == CMState::Dead {
            return;
        }

        self.current_term += 1;
        self.state = CMState::Candidate;
        self.voted_for = Some(self.id);
        self.votes_received = 1;  // 给自己投票
        self.leader_id = self.id;
        
        debug!("[{}] 开始选举: term={}", self.id, self.current_term);
        
        self.reset_election_timer();
    }
}

// /// WSL日志系统
// pub struct WSLSystem {
//     id: i32,                    // 节点id
//
// }

#[allow(dead_code)]
/// RaftCluster 管理一组 Raft 节点
pub struct RaftCluster {
    servers: Vec<Server>,
    ready_txs: Vec<mpsc::Sender<()>>,
    cm_list: Vec<Arc<Mutex<ConsensusModule>>>,
}

impl RaftCluster {
    /// 创建新的 Raft 集群并指定初始领导者, 以及有成为领导者资质的node_id
    pub async fn new_with_leader(leader_id: i32, leaders: Vec<i32>) -> Result<Self> {
        let mut cluster = Self::new().await?;
        for (_, &v) in leaders.iter().enumerate() {
            cluster.set_want_be_leader(v).expect("-------------not fount index 2-------------");
        }
        // 先将所有节点初始化为跟随者
        for i in 0..5 {
            let cm = cluster.get_cm(i)?;
            let mut cm = cm.lock().unwrap();
            cm.state = CMState::Follower;
            cm.current_term = 1;
            cm.voted_for = Some(leader_id);
            cm.reset_election_timer();
        }

        // 将指定节点设置为领导者
        {
            let cm = cluster.get_cm(leader_id)?;
            let mut cm = cm.lock().unwrap();
            cm.state = CMState::Leader;
            cm.current_term = 1;
            cm.voted_for = Some(leader_id);

            // 初始化领导者状态
            // TODO();
        }
        
        Ok(cluster)
    }

    /// 创建新的 Raft 集群
    pub async fn new() -> Result<Self> {
        let mut servers = Vec::new();
        let mut ready_txs = Vec::new();
        let mut cm_list = Vec::new();

        // 创建五个节点
        for i in 0..5 {
            let (ready_tx, _ready_rx) = mpsc::channel(1);
            let peer_ids: Vec<i32> = (0..5).filter(|&x| x != i).collect();
            
            let server = Server::new(i, peer_ids.clone());
            cm_list.push(Arc::clone(&server.cm));
            // if i == 2 {
            //     server.want_be_leader = true;
            //     cm.lock().unwrap().want_be_leader = true;
            // }
            ready_txs.push(ready_tx);
            servers.push(server);
        }

        // 创建RPC代理并设置到每个服务器
        let rpc_proxy = Arc::new(RPCProxy::new(cm_list.clone()));
        for server in &mut servers {
            server.set_rpc_proxy(Arc::clone(&rpc_proxy));
        }

        for server in &mut servers {
            server.serve().await?;
        }

        Ok(RaftCluster {
            servers,
            ready_txs,
            cm_list,
        })
    }

    // 设置某人想要变成leader
    pub fn set_want_be_leader(&mut self, index: i32) -> Result<()> {
        self.servers[index as usize].want_be_leader = true;
        {
            let tmp_clock = Arc::clone(&self.cm_list[index as usize]);
            let mut cm = tmp_clock.lock().unwrap();
            cm.want_be_leader = true;
        }
        Ok(())
    }

    /// 获取当前领导者
    pub fn get_leader(&self) -> Option<(i32, i32)> {
        for (i, cm) in self.cm_list.iter().enumerate() {
            let cm = cm.lock().unwrap();
            if cm.state == CMState::Leader {
                debug!("当前领导者: id={}, term={}", i, cm.current_term);
                return Some((i as i32, cm.current_term));
            }
        }
        debug_println(String::from("当前没有领导者"));
        debug!("当前没有领导者");
        None
    }

    /// 通过id查询对应共识协议的信息,
    /// 包括id, leader_id, current_term, state, peer_ids
    pub fn get_cm_info_by_id(&self, id: i32) -> Option<(i32, i32, i32, CMState, Vec<i32> )> {
        for (_, tmp_cm) in self.cm_list.iter().enumerate() {
            let cm = tmp_cm.lock().unwrap();
            if cm.id == id {
                return Some((cm.id, cm.leader_id, cm.current_term, cm.state, cm.peer_ids.clone()));
            }
        }
        None
    }

    /// 等待新的领导者被选出
    pub async fn wait_for_new_leader(&self, old_leader: i32, timeout_ms: u64) -> Option<(i32, i32)> {
        let start = Instant::now();
        let timeout = Duration::from_millis(timeout_ms);
        
        while start.elapsed() < timeout {
            if let Some((leader_id, term)) = self.get_leader() {
                if leader_id != old_leader {
                    debug!("发现新的领导者: id={}, term={}", leader_id, term);
                    return Some((leader_id, term));
                }
            }
            time::sleep(Duration::from_millis(50)).await;
        }
        
        debug!("等待新领导者超时: elapsed={:?}", start.elapsed());
        None
    }

    /// 停止指定节点的心跳发送
    pub fn stop_heartbeat(&self, node_id: i32) -> Result<()> {
        let cm = self.get_cm(node_id)?;
        let mut cm = cm.lock().unwrap();
        cm.state = CMState::Dead;  // 将节点设置为Dead状态
        cm.voted_for = None;  // 清除投票记录
        debug!("[{}] 节点已停止，状态设为Dead", node_id);
        Ok(())
    }

    /// 获取指定服务器的共识模块
    fn get_cm(&self, server_id: i32) -> Result<Arc<Mutex<ConsensusModule>>> {
        Ok(Arc::clone(&self.cm_list[server_id as usize]))
    }
}
