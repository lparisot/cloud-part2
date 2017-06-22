/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

typedef struct transactionInfo {
	MessageType messageType;
	int startTime;
	int replyNumber;
	string key;
	string value;
	bool failed;
} TransactionInfo;

#define REPLICA_NB 3
#define QUORUM ((REPLICA_NB/2)+1)
#define TIME_OUT 20
#define STABILIZER_ID -1

#define N_MINUS_2 	0
#define N_MINUS_1		1
#define N_PLUS_1		2
#define N_PLUS_2 		3

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	// transactions info
	map<int, TransactionInfo> transInfos;

private:
	bool isSameNode(Node first, Node second);
	int ifExistNode(vector<Node> v, Node n1);
	vector<pair<string, string>> findKeys(ReplicaType rep_type);
	void manageNeighbors();

	void clientCreateOrUpdate(string key, string value, MessageType type);
	void clientReadOrDelete(string key, MessageType type);

	void processCreateMessage(Message *receivedMessage);
	void processReadMessage(Message *receivedMessage);
	void processUpdateMessage(Message *receivedMessage);
	void processDeleteMessage(Message *receivedMessage);
	void processReadReplyMessage(Message *receivedMessage);
	void processReplyMessage(Message *receivedMessage);
	void pushNewTransactionInfo(string key, string value, int transID, MessageType messageType);

	void checkCoordinatorStatus();

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	vector<Node> findNeighbors(vector<Node> nodes);

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deleteKey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol(vector<Node> neighbors);

	~MP2Node();
};

#endif /* MP2NODE_H_ */
