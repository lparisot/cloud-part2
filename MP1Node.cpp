/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	// node is up
	memberNode->bFailed = false;
	memberNode->inited = true;

	memberNode->inGroup = false;
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
  initMemberListTable(memberNode);

  return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    if (isAddressLocale(joinaddr)) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
				sendGenericHearbeatRequest(JOINREQ, joinaddr);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
	memberNode->inited = false;

	return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: getNodeAddress
 *
 * DESCRIPTION: an helper to construct an Address from id and port
 */
Address MP1Node::getNodeAddress(int id, short port) {
	Address address;

	memset(&address, 0, sizeof(Address));
	*(int *)(&address.addr) = id;
	*(short *)(&address.addr[4]) = port;

	return address;
}

/**
 * FUNCTION NAME: isAddressLocale
 *
 * DESCRIPTION: Check if address is the locale one
 */
bool MP1Node::isAddressLocale(Address *address) {

	return (0 == memcmp((char *)&(memberNode->addr.addr),
											(char *)&(address->addr),
											sizeof(memberNode->addr.addr)));
}

/**
 * FUNCTION NAME: sendGenericHearbeatRequest
 *
 * DESCRIPTION: send our heartbeat to an other node
 */
void MP1Node::sendGenericHearbeatRequest(enum MsgTypes msgType, Address *to) {
	size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long);

	MessageHdr *msg;
	msg = (MessageHdr *) malloc(msgsize * sizeof(char));

	// message type
	msg->msgType = msgType;

	// our address
	memcpy((char *)(msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));

	memcpy((char *)(msg + 1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

	if (msgType == JOINREQ) {
#ifdef DEBUGLOG
		static char s[1024];
		sprintf(s, "Trying to join...");
		log->LOG(&memberNode->addr, s);
#endif
	}

	// send message
	emulNet->ENsend(&memberNode->addr, to, (char *)msg, msgsize);

	free(msg);
}

/**
 * FUNCTION NAME: sendMemberList
 *
 * DESCRIPTION: propagate our membership list
 */
void MP1Node::sendMemberList(enum MsgTypes msgType, Address *to) {
	int nb = memberNode->memberList.size();

	size_t msgsize = sizeof(MessageHdr) + sizeof(int)
			+ (nb *
				( sizeof(int)    	// id
				+ sizeof(short)  	// port
				+ sizeof(long)		// heartbeat
				+ sizeof(long)));	// timestamp

	MessageHdr *msg;
	msg = (MessageHdr *) malloc(msgsize * sizeof(char));

	// message type
	msg->msgType = msgType;

	// number of entries
	memcpy((char *)(msg + 1), &nb, sizeof(int));
	int offset = sizeof(int);

	// all entries
	for (	vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
				it != memberNode->memberList.end();
				++it) {
		memcpy((char *)(msg + 1) + offset, &it->id, sizeof(int));
		offset += sizeof(int);

		memcpy((char *)(msg + 1) + offset, &it->port, sizeof(short));
		offset += sizeof(short);

		memcpy((char *)(msg + 1) + offset, &it->heartbeat, sizeof(long));
		offset += sizeof(long);

		memcpy((char *)(msg + 1) + offset, &it->timestamp, sizeof(long));
		offset += sizeof(long);
	}

	emulNet->ENsend(&memberNode->addr, to, (char *)msg, msgsize);

	free(msg);
}

/**
 * FUNCTION NAME: updateMember
 *
 * DESCRIPTION: update a member or add it if not found in our list
 */
void MP1Node::updateMember(int id, short port, long heartbeat, long timestamp) {
#ifdef DEBUGLOG
	static char s[1024];
	sprintf(s, "updateMember %d:%d, %ld %ld", id, port, heartbeat, timestamp);
	log->LOG(&memberNode->addr, s);
#endif
	// if the entry is in our membership list, try to update it
	for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
			 it != memberNode->memberList.end();
			 ++it) {
		if ((id == it->id) && (port == it->port)) {
			if (heartbeat > it->heartbeat) {
				it->setheartbeat(heartbeat);
				it->settimestamp(timestamp);
			}
			return;
		}
	}

	// if not, add it
	MemberListEntry memberEntry(id, port, heartbeat, timestamp);
	memberNode->memberList.push_back(memberEntry);
	//memberNode->memberList.insert(memberNode->memberList.end(), memberEntry);

#ifdef DEBUGLOG
	Address address = getNodeAddress(id, port);
	log->logNodeAdd(&memberNode->addr, &address);
#endif
}
void MP1Node::updateMember(MemberListEntry& member) {
	updateMember(member.getid(), member.getport(), member.getheartbeat(), member.gettimestamp());
}

/**
 * FUNCTION NAME: addMemberList
 *
 * DESCRIPTION: add new members in our list.
 */
bool MP1Node::addMemberList(void *env, char *data, int size) {
	if (size < (int)(sizeof(int))) {
#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "ERROR addMemberList: message received with wrong size (number of members). Ignored.");
#endif
		return false;
	}

	// first retrieve the number of members
	int nb;
	memcpy(&nb, data + sizeof(MessageHdr), sizeof(int));
	int offset = sizeof(int);

	// we check if the size is correct
	if (size < (int)(nb * (sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long)))) {
#ifdef DEBUGLOG
		static char s[1024];
		sprintf(s, "ERROR addMemberList: message received with wrong size nb=%d size=%d. Ignored", nb, size);
		log->LOG(&memberNode->addr, s);
#endif
		return false;
	}

	MemberListEntry member;
	for (long i = 0; i < nb; i++) {
		memcpy(&member.id, data + sizeof(MessageHdr) + offset, sizeof(int));
		offset += sizeof(int);

		memcpy(&member.port, data + sizeof(MessageHdr) + offset, sizeof(short));
		offset += sizeof(short);

		memcpy(&member.heartbeat, data + sizeof(MessageHdr) + offset, sizeof(long));
		offset += sizeof(long);

		memcpy(&member.timestamp, data + sizeof(MessageHdr) + offset, sizeof(long));
		offset += sizeof(long);

		updateMember(member);
	}

	return true;
}

/**
 * FUNCTION NAME: onJoinReqMsg
 *
 * DESCRIPTION: a new member join. Add it to our list and send it our list.
 */
bool MP1Node::onJoinReqMsg(void *env, char *data, int size) {
	if (size < (int)(sizeof(memberNode->addr.addr) + sizeof(long))) {
#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "ERROR onJoinReqMsg Message received with wrong size. Ignored.");
#endif
		return false;
	}

	int id;
	short port;
	long heartbeat;
	memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
	memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
	memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(int) + sizeof(short), sizeof(long));

	updateMember(id, port, heartbeat, memberNode->timeOutCounter);

	Address sender = getNodeAddress(id, port);
	sendMemberList(JOINREP, &sender);

	return true;
}

/**
 * FUNCTION NAME: onJoinRepMsg
 *
 * DESCRIPTION: We receive a join response containings a list of members.
 *              Add it to our list.
 */
bool MP1Node::onJoinRepMsg(void *env, char *data, int size) {
	if (!addMemberList(env, data, size)) {
		return false;
	}

	return true;
}

/**
 * FUNCTION NAME: onHearbeatMsg
 *
 * DESCRIPTION: We receive a heartbeat message containings the hearbeat of
 *              the sender.
 *              Update our list, or add it to our list.
 */
bool MP1Node::onHearbeatMsg(void *env, char *data, int size) {
	if (size < (int)(sizeof(memberNode->addr.addr) + sizeof(long))) {
#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "ERROR manageHearbeatMsg Message received with wrong size. Ignored.");
#endif
		return false;
	}

	int id;
	short port;
	long heartbeat;
	memcpy(&id, data + sizeof(MessageHdr), sizeof(int));
	memcpy(&port, data + sizeof(MessageHdr) + sizeof(int), sizeof(short));
	memcpy(&heartbeat, data + sizeof(MessageHdr) + sizeof(int) + sizeof(short), sizeof(long));

	updateMember(id, port, heartbeat, memberNode->timeOutCounter);

	return true;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	MessageHdr * msg = (MessageHdr *)data;
	#ifdef DEBUGLOG
		 static char s[1024];
		 sprintf(s, "recvCallBack : %d, size=%d", data[0], size);
		 log->LOG(&memberNode->addr, s);
	#endif

	switch (msg->msgType) {
		case JOINREQ:
			log->LOG(&memberNode->addr, "Receive JOINREQ");
			return onJoinReqMsg(env, data, size);
		case JOINREP:
			log->LOG(&memberNode->addr, "Receive JOINREP");
			memberNode->inGroup = true;
			return onJoinRepMsg(env, data, size);
		case HEARTBEAT:
			log->LOG(&memberNode->addr, "Receive HEARBEAT");
			return onHearbeatMsg(env, data, size);
		default:
			log->LOG(&memberNode->addr, "Receive other");
			return false;
	}

	return true;
}

/**
 * FUNCTION NAME: checkFailure
 *
 * DESCRIPTION: Remove one node from the list if the timeout is passed
 */
void MP1Node::checkFailure() {
	for (	vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
				it != memberNode->memberList.end();
				++it) {
			Address address = getNodeAddress(it->getid(), it->getport());
			if (!isAddressLocale(&address)) {
				if (memberNode->timeOutCounter - it->gettimestamp() > TREMOVE) {
#ifdef DEBUGLOG
					log->logNodeRemove(&memberNode->addr, &address);
#endif
					memberNode->memberList.erase(it);
					break;
				}
			}
		}
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and
 * 				then delete the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
#ifdef DEBUGLOG
	static char s[1024];
#endif

	if (memberNode->pingCounter == 0) {
		// we need to send a new GOSSIP
		memberNode->heartbeat++;

		for (	vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
					it != memberNode->memberList.end();
					++it) {
			Address address = getNodeAddress(it->getid(),
																			 it->getport());
			if (!isAddressLocale(&address)) {
#ifdef DEBUGLOG
				sprintf(s, "Sending heartbeat to %s", address.getAddress().c_str());
				log->LOG(&memberNode->addr, s);
#endif
				sendGenericHearbeatRequest(HEARTBEAT, &address);
			}
		}

		memberNode->pingCounter = TFAIL;
	}
	else {
		memberNode->pingCounter--;
	}

	checkFailure();

	memberNode->timeOutCounter++;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	#ifdef DEBUGLOG
		log->LOG(&memberNode->addr, "initMemberListTable");
	#endif

	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                 addr->addr[3], *(short*)&addr->addr[4]) ;
}
