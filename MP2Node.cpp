/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	if(ring.size() > 0) {
		manageNeighbors();
	}

	vector<Node> neighbors = findNeighbors(curMemList);
	if(ring.size() >= 5 && haveReplicasOf.size() == 2 && hasMyReplicas.size() == 2) {
		if(!isSameNode(neighbors[N_MINUS_2], haveReplicasOf[0])) {
			change = true;
		}
		if(!isSameNode(neighbors[N_MINUS_1], haveReplicasOf[1])) {
			change = true;
		}
		if(!isSameNode(neighbors[N_PLUS_1], hasMyReplicas[0])) {
			change = true;
		}
		if(!isSameNode(neighbors[N_PLUS_2], hasMyReplicas[1])) {
			change = true;
		}
	}

	ring = curMemList;

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if(change == true && !ht->isEmpty()) {
		stabilizationProtocol(neighbors);
	}
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

void MP2Node::clientCreateOrUpdate(string key, string value, MessageType type) {
	vector<Node> replicas = findNodes(key);
	if(replicas.size() != REPLICA_NB) {
		return;
	}

	int transID = g_transID++;
	Message message(transID, getMemberNode()->addr, type, key, value);

    pushNewTransactionInfo(key, value, transID, type);

	message.replica = ReplicaType::PRIMARY;
	this->emulNet->ENsend(&memberNode->addr, replicas[0].getAddress(), message.toString());

	message.replica = ReplicaType::SECONDARY;
	this->emulNet->ENsend(&memberNode->addr, replicas[1].getAddress(), message.toString());

	message.replica = ReplicaType::TERTIARY;
	this->emulNet->ENsend(&memberNode->addr, replicas[2].getAddress(), message.toString());
}

void MP2Node::clientReadOrDelete(string key, MessageType type) {
	vector<Node> replicas = findNodes(key);
	if(replicas.size() != REPLICA_NB) {
		return;
	}

	int transID = g_transID++;
	Message message(transID, getMemberNode()->addr, type, key);

  pushNewTransactionInfo(key, "", transID, type);

	this->emulNet->ENsend(&memberNode->addr, replicas[0].getAddress(), message.toString());
	this->emulNet->ENsend(&memberNode->addr, replicas[1].getAddress(), message.toString());
	this->emulNet->ENsend(&memberNode->addr, replicas[2].getAddress(), message.toString());
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	clientCreateOrUpdate(key, value, MessageType::CREATE);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key) {
	clientReadOrDelete(key, MessageType::READ);
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value) {
	clientCreateOrUpdate(key, value, MessageType::UPDATE);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key) {
	clientReadOrDelete(key, MessageType::DELETE);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	// Insert key, value, replicaType into the hash table
	Entry entry(value, par->getcurrtime(), replica);
	string newValue = entry.convertToString();
	return ht->create(key, newValue);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	// Read key from local hash table and return value
	string value = ht->read(key);
	if (value.empty()) {
		return value;
	}
	Entry entry(value);
	return entry.value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	// Update key in local hash table and return true or false
	Entry entry(value, par->getcurrtime(), replica);
	string newValue = entry.convertToString();
	return ht->update(key, newValue);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deleteKey(string key) {
	// Delete the key from the local hash table
	return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	char * data;
	int size;
	Message *receivedMessage;

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		/*
		 * Handle the message types here
		 */
		string message(data, data + size);
		receivedMessage = new Message(message);

		switch(receivedMessage->type) {
			// server side messages
			case MessageType::CREATE:
				processCreateMessage(receivedMessage);
				break;
			case MessageType::READ:
				processReadMessage(receivedMessage);
				break;
			case MessageType::UPDATE:
				processUpdateMessage(receivedMessage);
				break;
			case MessageType::DELETE:
				processDeleteMessage(receivedMessage);
				break;

			// client side messages
			case MessageType::READREPLY:
				processReadReplyMessage(receivedMessage);
				break;
			case MessageType::REPLY:
				processReplyMessage(receivedMessage);
				break;
		}
		delete receivedMessage;
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
	checkCoordinatorStatus();
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol(vector<Node> neighbors) {
	///////////////////// Nodes for which I'm a replica
	// only old node -2 failed
	if(!isSameNode(haveReplicasOf[R_MINUS_2], neighbors[N_MINUS_2]) &&
					isSameNode(haveReplicasOf[R_MINUS_1], neighbors[N_MINUS_1])) {
		// done by code above
		log->LOG(&memberNode->addr, "stabilizationProtocol: only node -2 failed");
	}
	// old node -1 leave. old node -2 takes its place
	else if(isSameNode(haveReplicasOf[R_MINUS_2], neighbors[N_MINUS_1]) &&
					!isSameNode(haveReplicasOf[R_MINUS_2], neighbors[N_MINUS_2])) {
		// previous node was a primary
		// promote this one to primary replica (local)
		// promote N_PLUS_1 to secondary (update)
		// N_PLUS_2 become the tertiary (create)
		log->LOG(&memberNode->addr, "stabilizationProtocol: old node -1 leave. old node -2 takes its place");
		for(map<string, string>::iterator it=ht->hashTable.begin(); it != ht->hashTable.end(); it++) {
			Entry entry(it->second);
			string key = it->first;
			if(entry.replica == ReplicaType::SECONDARY) {
				// secondary data in this node will become primary
				// update local
				if(updateKeyValue(key, entry.value, PRIMARY)) {
					log->logUpdateSuccess(&memberNode->addr, false, STABILIZER_ID, key, entry.value);
				}
				else {
					log->logUpdateFail(&memberNode->addr, false, STABILIZER_ID, key, entry.value);
				}
				// old tertiary become secondary
				Message message(STABILIZER_ID, getMemberNode()->addr, MessageType::UPDATE, key, entry.value, ReplicaType::SECONDARY);
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_1].nodeAddress, message.toString());
				// a new tertiary is created
				message.type = MessageType::CREATE;
				message.replica = ReplicaType::TERTIARY;
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_2].nodeAddress, message.toString());
			}
		}
	}
	// both node -1 and -2 failed
	else if(!isSameNode(haveReplicasOf[R_MINUS_2], neighbors[N_MINUS_2]) &&
		 		  !isSameNode(haveReplicasOf[R_MINUS_1], neighbors[N_MINUS_1])) {
		// 2 previous nodes failed
		// promote this one to primary replica (local)
		// N_PLUS_1 become the secondary (update/create)
		// N_PLUS_2 become the tertiary (create)
		log->LOG(&memberNode->addr, "stabilizationProtocol: both node -1 and -2 failed");
		for(map<string, string>::iterator it=ht->hashTable.begin(); it != ht->hashTable.end(); it++) {
      Entry entry(it->second);
			string key = it->first;
      if(entry.replica == ReplicaType::SECONDARY) {
				// we were the secondary of node -1
				// local become primary
				if(updateKeyValue(key, entry.value, PRIMARY)) {
					log->logUpdateSuccess(&memberNode->addr, false, STABILIZER_ID, key, entry.value);
				}
				else {
					log->logUpdateFail(&memberNode->addr, false, STABILIZER_ID, key, entry.value);
				}
				// node +1 was tertiary and become secondary
				Message message(STABILIZER_ID, getMemberNode()->addr, MessageType::UPDATE, key, entry.value, ReplicaType::SECONDARY);
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_1].nodeAddress, message.toString());
				// create tertiary
				message.type = MessageType::CREATE;
				message.replica = ReplicaType::TERTIARY;
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_2].nodeAddress, message.toString());
			}
			if(entry.replica == ReplicaType::TERTIARY) {
				// we were the tertiary of node -2
				// local become primary
				if(updateKeyValue(key, entry.value, PRIMARY)) {
					log->logUpdateSuccess(&memberNode->addr, false, STABILIZER_ID, key, entry.value);
				} else {
					log->logUpdateFail(&memberNode->addr, false, STABILIZER_ID, key, entry.value);
				}
				// create secondary
				Message message(STABILIZER_ID, getMemberNode()->addr, MessageType::CREATE, key, entry.value, ReplicaType::SECONDARY);
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_1].nodeAddress, message.toString());
				// create tertiary
				message.replica = ReplicaType::TERTIARY;
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_2].nodeAddress, message.toString());
			}
		}
	}

	//////////////////////////// My replicas
	// only n+2 change
	if(!isSameNode(hasMyReplicas[R_PLUS_2], neighbors[N_PLUS_2]) &&
			isSameNode(hasMyReplicas[R_PLUS_1], neighbors[N_PLUS_1])) {
		// new node as tertiary
		log->LOG(&memberNode->addr, "stabilizationProtocol: only n+2 change");
		for(map<string, string>::iterator it=ht->hashTable.begin(); it != ht->hashTable.end(); it++) {
			Entry entry(it->second);
			if(entry.replica == ReplicaType::PRIMARY) {
				string key = it->first;
				// create
				Message message(STABILIZER_ID, getMemberNode()->addr, MessageType::CREATE, key, entry.value, ReplicaType::TERTIARY);
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_2].nodeAddress, message.toString());
			}
		}
	}
	// only n+1 leave
	else if(!isSameNode(hasMyReplicas[R_PLUS_1], neighbors[N_PLUS_1]) &&
		 isSameNode(hasMyReplicas[R_PLUS_2], neighbors[N_PLUS_1])) {
		log->LOG(&memberNode->addr, "stabilizationProtocol: only n+1 leave");
		for(map<string, string>::iterator it=ht->hashTable.begin(); it != ht->hashTable.end(); it++) {
			Entry entry(it->second);
			if(entry.replica == ReplicaType::PRIMARY) {
				string key = it->first;
				// old tertiary become secondary
				Message message(STABILIZER_ID, getMemberNode()->addr, MessageType::UPDATE, key, entry.value, ReplicaType::SECONDARY);
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_1].nodeAddress, message.toString());
				// new tertiary
				message.type = MessageType::CREATE;
				message.replica = ReplicaType::TERTIARY;
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_2].nodeAddress, message.toString());
			}
		}
	}
	// both nodes + 1 and + 2 change
	else if(!isSameNode(hasMyReplicas[R_PLUS_2], neighbors[N_PLUS_2]) &&
					!isSameNode(hasMyReplicas[R_PLUS_1], neighbors[N_PLUS_1])) {
		log->LOG(&memberNode->addr, "stabilizationProtocol: both nodes + 1 and + 2 change");
		for(map<string, string>::iterator it=ht->hashTable.begin(); it != ht->hashTable.end(); it++) {
			Entry entry(it->second);
			if(entry.replica == ReplicaType::PRIMARY) {
				string key = it->first;
				// create new secondary
				Message message(STABILIZER_ID, getMemberNode()->addr, MessageType::CREATE, key, entry.value, ReplicaType::SECONDARY);
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_1].nodeAddress, message.toString());
				// create new tertiary
				message.replica = ReplicaType::TERTIARY;
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_2].nodeAddress, message.toString());
			}
		}
	}
	// new n+1 between us and old n+1
	if(!isSameNode(hasMyReplicas[R_PLUS_1], neighbors[N_PLUS_1]) &&
		 isSameNode(hasMyReplicas[R_PLUS_1], neighbors[N_PLUS_2])) {
		log->LOG(&memberNode->addr, "stabilizationProtocol: new n+1 between us and old n+1");
		for(map<string, string>::iterator it=ht->hashTable.begin(); it != ht->hashTable.end(); it++) {
			Entry entry(it->second);
			if(entry.replica == ReplicaType::PRIMARY) {
				string key = it->first;
				// new secondary
				Message message(STABILIZER_ID, getMemberNode()->addr, MessageType::CREATE, key, entry.value, ReplicaType::SECONDARY);
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_1].nodeAddress, message.toString());
				// old secondary become tertiary
				message.type = MessageType::UPDATE;
				message.replica = ReplicaType::TERTIARY;
				this->emulNet->ENsend(&memberNode->addr, &neighbors[N_PLUS_2].nodeAddress, message.toString());
				// delete old  tertiary
				message.type = MessageType::DELETE;
				message.replica = ReplicaType::TERTIARY;
				this->emulNet->ENsend(&memberNode->addr, &hasMyReplicas[R_PLUS_2].nodeAddress, message.toString());
			}
		}
	}
}

void MP2Node::manageNeighbors() {
	vector<Node> neighbors = findNeighbors(ring);
	haveReplicasOf.clear();
	haveReplicasOf.push_back(neighbors[N_MINUS_2]);
	haveReplicasOf.push_back(neighbors[N_MINUS_1]);
	hasMyReplicas.clear();
	hasMyReplicas.push_back(neighbors[N_PLUS_1]);
	hasMyReplicas.push_back(neighbors[N_PLUS_2]);
}
/**
	* FUNCTION NAME: processCreateMessage
	*
	* DESCRIPTION: send create reply message
	*
*/
void MP2Node::processCreateMessage(Message *receivedMessage) {
	bool isCreatedSuccessfully = createKeyValue(
		receivedMessage->key,
		receivedMessage->value,
		receivedMessage->replica
	);

	if(isCreatedSuccessfully) {
		log->logCreateSuccess(&this->memberNode->addr, false, receivedMessage->transID, receivedMessage->key, receivedMessage->value);
	}
	else {
		log->logCreateFail(&this->memberNode->addr, false, receivedMessage->transID, receivedMessage->key, receivedMessage->value);
	}

	if(receivedMessage->transID != STABILIZER_ID) {
		Message reply(receivedMessage->transID, getMemberNode()->addr, MessageType::REPLY, isCreatedSuccessfully);

		this->emulNet->ENsend(&memberNode->addr, &receivedMessage->fromAddr, reply.toString());
	}
}

/**
	* FUNCTION NAME: processReadMessage
	*
	* DESCRIPTION: send read reply message
	*
*/
void MP2Node::processReadMessage(Message *receivedMessage) {
	string value = readKey(receivedMessage->key);

	if(!value.empty()) {
		log->logReadSuccess(&this->memberNode->addr, false, receivedMessage->transID, receivedMessage->key, value);
	}
	else {
		log->logReadFail(&this->memberNode->addr, false, receivedMessage->transID, receivedMessage->key);
	}

	Message reply(receivedMessage->transID, getMemberNode()->addr, value);
	this->emulNet->ENsend(&memberNode->addr, &receivedMessage->fromAddr, reply.toString());
}

/**
	* FUNCTION NAME: processUpdateMessage
	*
	* DESCRIPTION: send update reply message
	*
*/
void MP2Node::processUpdateMessage(Message *receivedMessage) {
	bool isUpdatedSuccessfully = updateKeyValue(
		receivedMessage->key,
		receivedMessage->value,
		receivedMessage->replica
	);

	if(isUpdatedSuccessfully) {
		log->logUpdateSuccess(&this->memberNode->addr, false, receivedMessage->transID, receivedMessage->key, receivedMessage->value);
	}
	else {
		log->logUpdateFail(&this->memberNode->addr, false, receivedMessage->transID, receivedMessage->key, receivedMessage->value);
	}

	Message reply(receivedMessage->transID, getMemberNode()->addr, MessageType::REPLY, isUpdatedSuccessfully);
	this->emulNet->ENsend(&memberNode->addr, &receivedMessage->fromAddr, reply.toString());
}

/**
	* FUNCTION NAME: processDeleteMessage
	*
	* DESCRIPTION: send delete reply message
	*
*/
void MP2Node::processDeleteMessage(Message *receivedMessage) {
	bool isDeletedSuccessfully = deleteKey(receivedMessage->key);

	if(isDeletedSuccessfully) {
		log->logDeleteSuccess(&this->memberNode->addr, false, receivedMessage->transID, receivedMessage->key);
	}
	else {
		log->logDeleteFail(&this->memberNode->addr, false, receivedMessage->transID, receivedMessage->key);
	}

  Message reply(receivedMessage->transID, getMemberNode()->addr, MessageType::REPLY, isDeletedSuccessfully);
	this->emulNet->ENsend(&memberNode->addr, &receivedMessage->fromAddr, reply.toString());
}

/**
	* FUNCTION NAME: processReadReplyMessage
	*
	* DESCRIPTION: managed read reply message
	*
*/
void MP2Node::processReadReplyMessage(Message *receivedMessage) {
	map<int, TransactionInfo>::iterator it;
	it = transInfos.find(receivedMessage->transID);
	if(it != transInfos.end()) {
		if(receivedMessage->value != "") {
			it->second.value = receivedMessage->value;
			it->second.replyNumber++;
		}
		else {
			it->second.failed = true;
		}
	}
}

/**
	* FUNCTION NAME: processReplyMessage
	*
	* DESCRIPTION: managed reply message
	*
*/
void MP2Node::processReplyMessage(Message *receivedMessage) {
	map<int, TransactionInfo>::iterator it;
	it = transInfos.find(receivedMessage->transID);
	if(it != transInfos.end()) {
		if(receivedMessage->success) {
			it->second.replyNumber++;
		}
		else {
			it->second.failed = true;
		}
	}
}

/**
	* FUNCTION NAME: pushNewTransactionInfo
	*
	* DESCRIPTION: store a new transaction
	*
*/
void MP2Node::pushNewTransactionInfo(string key, string value, int transID, MessageType messageType) {
	TransactionInfo transInfo;
	transInfo.replyNumber = 0;
	transInfo.messageType = messageType;
	transInfo.startTime = par->getcurrtime();
	transInfo.key = key;
	transInfo.value = value;
	transInfo.failed = false;
	transInfos.emplace(transID, transInfo);
}

/**
	* FUNCTION NAME: checkCoordinatorStatus
	*
	* DESCRIPTION: check all transactions
	*
*/
void MP2Node::checkCoordinatorStatus() {
	map<int, TransactionInfo>::iterator it;
	for(it = transInfos.begin(); it != transInfos.end();) {
		int transID = it->first;
		TransactionInfo transInfo = it->second;
		switch(transInfo.messageType) {
			case MessageType::CREATE:
				if(par->globaltime - transInfo.startTime > TIME_OUT || transInfo.failed) {
					log->logCreateFail(&getMemberNode()->addr, true, transID, transInfo.key, transInfo.value);
					it = transInfos.erase(it);
				}
				else {
					if(transInfo.replyNumber == REPLICA_NB) {
						log->logCreateSuccess(&getMemberNode()->addr, true, transID, transInfo.key, transInfo.value);
						it = transInfos.erase(it);
					}
					else {
						++it;
					}
				}
				break;
			case MessageType::UPDATE:
				if(par->globaltime - transInfo.startTime > TIME_OUT || transInfo.failed) {
					log->logUpdateFail(&getMemberNode()->addr, true, transID, transInfo.key, transInfo.value);
					it = transInfos.erase(it);
				}
				else {
					if(transInfo.replyNumber >= QUORUM) {
						log->logUpdateSuccess(&getMemberNode()->addr, true, transID, transInfo.key, transInfo.value);
						it = transInfos.erase(it);
					}
					else {
						++it;
					}
				}
				break;
			case MessageType::READ:
				if(par->globaltime - transInfo.startTime > TIME_OUT || transInfo.failed) {
					log->logReadFail(&getMemberNode()->addr, true, transID, transInfo.key);
					it = transInfos.erase(it);
				}
				else {
					if(transInfo.replyNumber >= QUORUM) {
						log->logReadSuccess(&getMemberNode()->addr, true, transID, transInfo.key, transInfo.value);
						it = transInfos.erase(it);
					}
					else {
						++it;
					}
				}
				break;
			case MessageType::DELETE:
				if(par->globaltime - transInfo.startTime > TIME_OUT || transInfo.failed) {
					log->logDeleteFail(&getMemberNode()->addr, true, transID, transInfo.key);
					it = transInfos.erase(it);
				}
				else {
					if(transInfo.replyNumber == REPLICA_NB) {
						log->logDeleteSuccess(&getMemberNode()->addr, true, transID, transInfo.key);
						it = transInfos.erase(it);
					}
					else {
						++it;
					}
				}
				break;
			default:
				break;
		}
	}
}

vector<Node> MP2Node::findNeighbors(vector<Node> nodes) {
	vector<Node>::iterator forwardNode, backwardNode;

	bool find = false;
	// search actual node
	for(vector<Node>::iterator it=nodes.begin(); it != nodes.end(); it++) {
		if(it->nodeAddress == memberNode->addr) {
			forwardNode = it;
			backwardNode = it;
      find = true;
			break;
		}
	}

	vector<Node> neighbors(4);

	if(find) {
		if(backwardNode == nodes.begin()) {
			backwardNode = nodes.end();
		}
		backwardNode--;
		neighbors[N_MINUS_1] = *backwardNode;

		if(backwardNode == nodes.begin()) {
			backwardNode = nodes.end();
		}
		backwardNode--;
		neighbors[N_MINUS_2] = *backwardNode;

		forwardNode++;
		if(forwardNode == nodes.end()) {
			forwardNode = nodes.begin();
		}
		neighbors[N_PLUS_1] = *forwardNode;

		forwardNode++;
		if(forwardNode == nodes.end()) {
			forwardNode = nodes.begin();
		}
		neighbors[N_PLUS_2] = *forwardNode;
	}

	return neighbors;
}

bool MP2Node::isSameNode(Node first, Node second) {
	return (first.getHashCode() == second.getHashCode());
}
