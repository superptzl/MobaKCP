package com.core.net;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.mina.core.buffer.IoBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Kcp implements Runnable{
	
	private final static Logger logs = LoggerFactory.getLogger(Kcp.class);
	
	private MinaIoSession minaSession = null;
	/** 运行标记  **/
	public boolean running = false;
	/** 发送窗口大小   **/
	public static final int IKCP_SEND_WND = 32;
	/** 接收窗口大小  **/
	public static final int IKCP_RECV_WND = 32;
	
	/** KCP 头长度**/
	public static final byte IKCP_OVERHEAD = 22;
	/** 发送包 **/
	public static final byte IKCP_CMD_PUSH = 1;
	/** 确认包  **/
	public static final byte IKCP_CMD_ACK = 2;
	/** 问寻接收窗口大小 **/
    public static final byte IKCP_CMD_WASK = 83;
    /** 告之接收窗口大小**/
    public static final byte IKCP_CMD_WINS = 84;
    /** 最小7秒问寻一次接收窗口大小**/
    public static final long IKCP_PROBE_INIT = 7000;
    /** 最大60 * 5 秒时长**/
    public static final long IKCP_PROBE_LIMIT = 60 * 1000 * 5; 
	/** 重传时间 **/
	public static final int IKCP_RTO_MIN = 100;
    public static final int IKCP_RTO_MAX = 1000 * 60;
    /** 线程执行间隔  **/
    public static final long THREAD_INTERVAL = 33;
    /** 问寻间隔 **/
    private long probeWait = 0l;
    /** 问寻目标时间**/ 
    private long tsProbe = 0l;
	
	private final Queue<KcpPacket> received;//输入队列
	private final Queue<KcpPacket> sendList;//输出队列
	
	/** 待发送的消息队列  **/
	private final LinkedList<KcpPacket> sndQueue = new LinkedList<>(); 
	/** 接收消息的队列 **/
	private final LinkedList<KcpPacket> rcvQueue = new LinkedList<>();
	/** 发送缓存  ，数据的sn 可能是间隔的   **/
	private final HashMap<Integer , KcpPacket> sndBuf = new HashMap<>();
	/** 接收缓存  ，数据的sn 可能是间隔的   **/
	private final HashMap<Integer , KcpPacket> rcvBuf = new HashMap<>();
	/** 确认包数据  SN**/
	private final LinkedList<Integer> ackSnList = new LinkedList<>();
	/** 确认包数据  TS**/
	private final LinkedList<Long> ackTsList = new LinkedList<>();
	/** 发送窗口大小**/
	private int sendWnd = 0;
	/** 接收窗口大小**/
	private int recvWnd = 0;
	/** 发送的最大顺序号  **/
	private int sendNext = 0; 
	/** 接收的最大顺序号  **/
	private int recvNext = 0;
	/** 远端最大连续确认号**/
	private int remoteSn = 0;
	/** 远端最小连续确认号   **/
	private int remoteMinSn = 0;
	/** 当前时间 **/
	private long currTime = 0;
	/** kcp 起动的时间，超过1分钟没动作过就关闭**/
	private long kcpStartTime = 0;
	/** kcp 起动的时间，超过1分钟没动作过就关闭**/
	private long kcpEndTime = 0;
	/** rtt 平均时间 (ms)  **/
	private int rxSrtt = 0;
	/** rtt时间间隔 (ms) **/
	private int rxRttval = 0;
	/** rto时间 ms**/
	private int rxRto = 0;
		
	public Kcp(MinaIoSession session){
		this.received = new LinkedBlockingQueue<>();
	    this.sendList = new LinkedBlockingQueue<>();
	    this.sendWnd = IKCP_SEND_WND ;
	    this.recvWnd = IKCP_RECV_WND;
		this.minaSession = session;
		this.running = true;
		this.sendNext = 0;
		this.recvNext = 0;
		this.kcpStartTime = System.currentTimeMillis();
		this.kcpEndTime = System.currentTimeMillis();
	}
	
	public void update(){
		//接收的数据待处理
		while (!this.received.isEmpty()){
			KcpPacket kpt = this.received.remove();
			rcvQueue.add(kpt);
        }
		//本地输入待处理
		while (!this.sendList.isEmpty()){
            KcpPacket kpt = this.sendList.remove();
            sndQueue.add(kpt);
        }
		//处理要接收的
		processRecv();
		//处理要发的确认包
		processAck();
		//问寻窗口
		processWask();
		//处理要发送的	
		processSend();		
		//处理状态
		processStatus();
	}
	
	private void processWask(){
		if (sendWnd == 0){
            if (probeWait == 0){
            	probeWait = IKCP_PROBE_INIT;
                tsProbe = currTime + probeWait;
            }else if (currTime - tsProbe >= 0){
                if (probeWait < IKCP_PROBE_INIT){
                	probeWait = IKCP_PROBE_INIT;
                }
                probeWait += probeWait / 2;
                if (probeWait > IKCP_PROBE_LIMIT){
                	probeWait = IKCP_PROBE_LIMIT;
                }
                tsProbe = currTime + probeWait;                
            }
        }else{
        	tsProbe = 0;
            probeWait = 0;
        }
	} 
	
	private void processStatus(){
		if (currTime - this.kcpEndTime > IKCP_PROBE_LIMIT + 100){			
			this.minaSession.close();
		}
	}
	
	public void processRecv(){
		int count = rcvQueue.size();					
		for (int i = 0 ; i < count ; i++){				
			KcpPacket kpt = rcvQueue.remove();				
			if (kpt.getCmd() == IKCP_CMD_PUSH){				
				if (kpt.getSn() >= recvNext){ //必需要大等于最后一个可收号才是有效的					
					rcvBuf.put(kpt.getSn() , kpt);
					checkSendWnd(kpt.getWin());
					//加到确认列表中
					addAckList(kpt.getSn() , kpt.getTs());

					logs.info("[RCV_PROCESS]CMD[" + kpt.getCmd() + "]SN[" + kpt.getSn() + "]OK");
				}else{
					logs.info("[RCV_PROCESS]CMD[" + kpt.getCmd() + "]SN[" + kpt.getSn() + "]FAIL");
				}
			}else if(kpt.getCmd() == IKCP_CMD_ACK){
				parseAck(kpt);
				checkSendWnd(kpt.getWin());
			}else if(kpt.getCmd() == IKCP_CMD_WASK){
				parseWask();
			}else if(kpt.getCmd() == IKCP_CMD_WINS){
				parseWins(kpt);
			}
			
			if (kpt.getNextSn() > remoteSn){
				remoteSn = kpt.getNextSn();
			}
		}
		
		if (count > 0){
			setLastActiveTime(currTime);
		}
		
		//将数据包顺序加入到结果中 , 并加到确认列表中
//		for (KcpPacket kpt : rcvBuf.values()){			
//			addAckList(kpt.getSn() , kpt.getTs());
//		}
		count = rcvBuf.size();
		for (int i = recvNext; i < recvNext + count  ; i++){
			KcpPacket kpt = rcvBuf.get(recvNext);
			if (kpt != null){
				rcvBuf.remove(kpt.getSn());
				recvKcp(kpt);
				checkSendWnd(kpt.getWin());
				recvNext ++;
			}
		}
		//设置当前窗口大小
		recvWnd = IKCP_SEND_WND - rcvBuf.size();
		if (recvWnd == 0){
//			recvWnd = 1;
			logs.error("[RECV_IS_FULL]");
		}
	}
	
	private void parseWask(){
		KcpPacket kpt = new KcpPacket();
		kpt.setCmd(Kcp.IKCP_CMD_WINS);
		kpt.setWin((byte)recvWnd);
		kpt.setNextSn(recvNext);
		sendKcp(kpt);	
		logs.info("[WASK]SEND");
	}
	
	/**
	 * 检查跳过的包
	 */
	public void checkSkip(int sn){
		for (KcpPacket kpt : sndBuf.values()){
			if (kpt.getSn() < sn){
				kpt.increaSkip();
			}
		}
	}
	
	public void processSend(){
		//sendWnd = sendWnd == 0 ? 1 : sendWnd;
		//发送问寻
		if (tsProbe > 0 && currTime > tsProbe){
			KcpPacket wkpt = new KcpPacket();
			wkpt.setCmd(Kcp.IKCP_CMD_WASK);			
			wkpt.setWin((byte)recvWnd);
			wkpt.setNextSn(recvNext);
			sendKcp(wkpt);			
		}
		
		// 按窗口大小处理包
		if (sendWnd > 0) {
			int count = sndQueue.size();
			for (int i = 0; i < count ; i++) {
				if (sendWnd > 0) {
					KcpPacket kpt = sndQueue.remove();
					if (kpt.getCmd() == IKCP_CMD_PUSH) {
						kpt.setSn(sendNext);
						kpt.setTs(currTime);
						sndBuf.put(kpt.getSn(), kpt);
						sendNext++;
						sendWnd--;
					}
				}else{
					break;
				}
			}
		}else{
			logs.info("[PROCESS_SEND]WND[" + sendWnd + "]SEND_WND_ZERO");
			return;
		}
		//已经确认，不用再重发的包sn
		List<Integer> removeSnLst = new ArrayList<Integer>(); 
		//处理发送，重发
		for (KcpPacket kpt : sndBuf.values()){
			kpt.setWin((byte)recvWnd);//这里是告之接收方，当前发送方的可接收窗口大小
			kpt.setNextSn(recvNext);
			//如果序号小于远程最大确认包，说明这个确认晚了，远程已经有此包，不用重发
			if (kpt.getSn() < remoteSn){
				removeSnLst.add(kpt.getSn());
				continue;
			}			
			if (kpt.getTs() >= currTime && kpt.getRto() == 0){//新包直接发送
				sendKcp(kpt);
			}else{
				if (remoteSn < kpt.getSn()){
					if (kpt.getSkip() >= 3){
						kpt.setTs(currTime);
						kpt.setSkip(0);
//						sendKcp(kpt);
						logs.info("[FAST_SEND_KCP]CMD[" + kpt.getCmd()+"]SN[" + kpt.getSn() + "]SKIP[" + kpt.getSkip() + "]");
						continue;
					}					
				}
				//收到对特定报文段的确认之前计时器超时，则重传该报文，并且进行RTO = 2 * RTO进行退避。这是TCP的，咱们使用网上给出的值1.5
				if (rxRto == 0) rxRto = 1000;
				long time = (long)(1.5 * (kpt.getRto() + 1) * rxRto) + THREAD_INTERVAL;				
				time = time > IKCP_RTO_MAX ? IKCP_RTO_MAX : time;	
				time = time == 0 ? IKCP_RTO_MIN : time;
				//超时重发
				if (currTime > time + kpt.getTs()){
					kpt.setTs(currTime);
					logs.info("[RE_SEND_KCP]CMD[" + kpt.getCmd()+"]SN[" + kpt.getSn() + "]RE_TIME[" + time + "]");
//					sendKcp(kpt);
					kpt.increaRto();
					kpt.setSkip(0);
				}
				
			}
		}
		for (int sn : removeSnLst){
			sndBuf.remove(sn);
		}		
	}
	
	/**
	 * 处理确认包 ，一次性可以确认多个数据
	 */
	public void processAck(){
		int count = ackSnList.size();
		if (count <= 0) return;
		final IoBuffer io = IoBuffer.allocate(count * 12);
		for (int i = 0 ; i < count ; i++){
			io.putInt(ackSnList.get(i));//sn
			io.putLong(ackTsList.get(i));//ts
		}
		io.flip();
		byte[] data = new byte[count * 12];
		io.get(data , 0 , io.remaining());
		
		KcpPacket kpt = new KcpPacket();
		kpt.setCmd(Kcp.IKCP_CMD_ACK);
		kpt.setData(data);		
		kpt.setWin((byte)recvWnd);
		kpt.setNextSn(recvNext);
		kpt.setTs(currTime);
		sendKcp(kpt);
		ackSnList.clear();
		ackTsList.clear();
	}
	
	
	/** 收到窗口问寻返回包  **/
	private void parseWins(KcpPacket kpt){
		checkSendWnd(kpt.getWin());
		logs.info("[WINS]WIN[" + kpt.getWin() + "]");
	}
	
	/** 收到确认包  **/
	private void parseAck(KcpPacket kpt){
		long minTs = 0; 
		final IoBuffer io = IoBuffer.allocate(kpt.getData().length);
		io.put(kpt.getData());
		io.flip();
		while (io.remaining() > 0){
			int sn = io.getInt();
			long ts = io.getLong();
			sndBuf.remove(sn); // 将确认的包从待发队列中去掉
			//检查跳过包
			checkSkip(sn);
			logs.info("[MINTS]SN[" + sn + "]TS[" + ts + "]CUR[" + currTime + "]RET[" + (currTime - ts) + "]");
			if (minTs == 0 || minTs < ts){
				minTs = ts;
			}
		}
		if (currTime - minTs >= 0){
			updateRto((int)(currTime - minTs));
        }
	}
	
	private void checkSendWnd(int wnd){
		if (wnd > IKCP_SEND_WND){
			wnd = IKCP_SEND_WND;
			logs.info("[WND_TOO_LARGE]" + wnd);
		}
		if (sendWnd < wnd){
			sendWnd = wnd;
		}
	}
	
	@Override
	public void run() {
		while(this.running){
			try{
				//10ms一次调用
				currTime = System.currentTimeMillis();
				update();
				Thread.sleep(THREAD_INTERVAL);
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	/**
	 * 给应用层的发包接口，实际发送由KCP来控制
	 * @param pk
	 */
	public void send(Packet pk){
		KcpPacket kpt = new KcpPacket();
		kpt.setCmd(Kcp.IKCP_CMD_PUSH);
		kpt.setData(pk.getPacket());
		sendList.add(kpt);
	}
	/**
	 * 实际调用发送
	 * @param pk
	 */
	public void sendKcp(KcpPacket pk){
		minaSession.session.write(pk);
		logs.info("[SEND_KCP]CMD[" + pk.getCmd()+"]SN[" + pk.getSn() + "]TS[" + pk.getTs() + "]");
	}
	/**
	 * 给应用层的收包接口，实际收到由KCP来控制
	 * @param pk
	 */
	public void recv(KcpPacket pk){
		received.add(pk);
	}
	
	/**
	 * 实际调用收包
	 * @param pk
	 */
	public void recvKcp(KcpPacket kpt){
		List<Packet> lst = kpt.getPacket();
		if (lst != null){
			for (Packet pk : lst){
				minaSession.receive(pk);
				logs.info(pk.getMsgId() + " " + pk.getJson());
			}
		}
	}
	/**
	 * 接收有效数据包后 ，将sn,ts 添加到确认队列，后面会回复给发送方以便确认。
	 * @param sn
	 * @param ts
	 */
	public void addAckList(int sn , long ts){
	    ackSnList.add(sn);
	    ackTsList.add(ts);
	}
	
	/** 
	 * 设置最好活跃时间
	 * **/
	private void setLastActiveTime(long time){
		this.kcpEndTime = time;
	}
	
	/** 计算rto 时间  **/
    private void updateRto(int rtt)
    {    	
        if (rxSrtt == 0)
        {
            rxSrtt = rtt;
            rxRttval = rtt / 2;//rtt 一来一回的时间ms
        } else
        {
            long delta = rtt - rxSrtt;
            if (delta < 0)
            {
                delta = -delta;
                logs.info("[UPDATA_ACK_ERR]DELTA[" + delta + "]RTT[" + rtt + "]RXRTT[" + rxSrtt + "]");
                delta = 33;
            }
            
//          由于路由器的拥塞和端系统负载的变化，由于这种波动，用一个报文段所测的SampleRTT来代表同一段时间内的RTT总是非典型的，为了得到一个典型的RTT，
//          TCP规范中使用低通过滤器来更新一个被平滑的RTT估计器。
//          TCP维持一个估计RTT（称之为EstimatedRTT），一旦获得一个新SampleRTT时，则根据下式来更新EstimatedRTT： 
//          EstimatedRTT = （1-a）* EstimatedRTT + a * SampleRTT 
//          其中a通常取值为0.125，即：
//          EstimatedRTT = 0.875 * EstimatedRTT + 0.125 * SampleRTT
//          每个新的估计值的87.5%来自前一个估计值，而12.5%则取自新的测量。            
            rxSrtt = (7 * rxSrtt + rtt) / 8;

//          由于新测量SampleRTT的权值只占EstimatedRTT的12.5%，当实际RTT变化很大的时候，即便测量到的SampleRTT变化也很大，但是所占比重小，
//          最后EstimatedRTT的变化也不大，从而RTO的变化不大，造成RTO过小，容易引起不必要的重传。因此对RTT的方差跟踪则显得很有必要。 
//          在TCP规范中定义了RTT偏差DevRTT，用于估算SampleRTT一般会偏离EstimatedRTT的程度：
//          DevRTT = (1-B)*DevRTT + B*|SampleRTT - EstimatedRTT|
//          其中B的推荐值为0.25，当RTT波动很大的时候，DevRTT的就会很大。
            rxRttval = (int)((3 * rxRttval + delta) / 4);
            
            if (rxSrtt < 1)
            {
            	logs.info("[UPDATA_ACK_ERR]RTT[" + rtt + "]RXRTT[" + rxSrtt + "]");
                rxSrtt = 1;
            }
        }
//      重传时间间隔RTO的计算公式为：
//      RTO = EstimatedRTT + 4 * DevRTT
        int rto = rxSrtt + Math.max(IKCP_RTO_MIN , 4 * rxRttval);
        rxRto = ibound(IKCP_RTO_MIN , rto , IKCP_RTO_MAX);
        logs.info("[CALC_RTO]RTO[" + rxRto + "]");
    }
    
    private int ibound(int lower, int middle, int upper)
    {
        return Math.min(Math.max(lower, middle), upper);
    }
}
