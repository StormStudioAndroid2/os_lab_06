#include <iostream>
#include "zmq.hpp"
#include <string>
#include "zconf.h"
#include <vector>
#include <signal.h>
#include <sstream>
#include <set>
#include <algorithm>
#include "server.h"



struct TNode {
    unsigned long long Id; 
    TNode* Left;
    long long Height;
    TNode* Right;
TNode(unsigned long long  id) {
    Id = id;   
    Height = 1;
    Left = 0;
    Right = 0;  
}

};

long long GetHeight(TNode* n) {
    if (n!=NULL) {
        return n->Height;
    }
    return 0;
}
long long GetBalance(TNode* n) {
    return GetHeight(n->Right)-GetHeight(n->Left);
}

void CountHeight(TNode* n)
{
	int hl = GetHeight(n->Left);
	int hr = GetHeight(n->Right);
	n->Height = (hl>hr?hl:hr)+1;
}
TNode* RotateLeft(TNode* q) {
    TNode*p = q->Right;

    q->Right = p->Left;
    p->Left = q;
    CountHeight(q);
    CountHeight(p);
    return p;

}
TNode* RotateRight(TNode* q) {
    TNode* p = q->Left;
    q->Left = p->Right;
    p->Right = q;
    CountHeight(q);
    CountHeight(p);
    return p;
}
TNode* BalanceTree(TNode* p) {
    CountHeight(p);
    if (GetBalance(p)==2) {
        TNode* q = p->Right;
        if (GetBalance(q)<0) {
            p->Right = RotateRight(q);
        } 
        return RotateLeft(p);
    }
    if (GetBalance(p)==-2) {
        TNode* q = p->Left;
        if (GetBalance(q)>0) {
            p->Left=RotateLeft(q);
        } 
        return RotateRight(p);
    }
    return p;
}
   void get_nodes1(TNode* node, std::vector<int>& v)  {
        if (node == nullptr) {
            return;
        }
        get_nodes1(node->Left,v);
        v.push_back(node->Id);
        get_nodes1(node->Right, v);
    }
 std::vector<int> get_nodes(TNode* head)  {
        std::vector<int> result;
        get_nodes1(head, result);
        return result;
    }
 
TNode* AddElement(TNode* p, unsigned long long k) {
	if( !p ) {
        return new TNode(k);   
    }
    if (k<p->Id) {
		p->Left = AddElement(p->Left,k);
    }
	else if (k>p->Id) {
		p->Right = AddElement(p->Right,k);
    }
	return BalanceTree(p);
}
TNode* FindMinimum(TNode* p)  {
    if (p->Left==0) {
        return p;
    }
    return FindMinimum(p->Left);
}
TNode* RemoveMinimum(TNode* p) 
{
	if( p->Left==0 ) {
		return p->Right;
    }
	p->Left = RemoveMinimum(p->Left);
	return BalanceTree(p);
}
TNode* RemoveElement(TNode* p, unsigned long long k) {
    if (!p) {
        return 0;
    }
	if(k<p->Id) {
		p->Left = RemoveElement(p->Left,k);
    }
	else if(k>p->Id) {
		p->Right = RemoveElement(p->Right,k);
    }
	if (k==p->Id) {
        TNode* q = p->Left;
		TNode* r = p->Right;
		delete p;
        if (r==0) {
            return q;
        }
		TNode* min = FindMinimum(r);
		min->Right = RemoveMinimum(r);
		min->Left = q;
		return BalanceTree(min);
    }
    return BalanceTree(p);
	
}
void RemoveTree(TNode* p) {
    if (!p) {
        return;
    }
    RemoveTree(p->Left);
    RemoveTree(p->Right);
    delete p;
}
TNode* FindById(TNode* p, unsigned long long Id) {
    if (p==0) {
        return 0;
    }
  
    if (Id<p->Id) {
        return FindById(p->Left,Id);
    }
    if (Id>p->Id) {
        return FindById(p->Right,Id);
    }
    return p;
}


int main() {
    std::string command;
    TNode* head  = 0;
    size_t child_pid = 0;
    int child_id = 0;
    zmq::context_t context(1);
    zmq::socket_t main_socket(context, ZMQ_REQ);
    int linger = 0;
    main_socket.setsockopt(ZMQ_SNDTIMEO, 2000);
    //main_socket.setsockopt(ZMQ_RCVTIMEO, 2000);
    main_socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));
    //main_socket.connect(get_connect_name(30000));
    int port = bind_socket(main_socket);

    while (true) {
        std::cin >> command;
        if (command == "create") {
            size_t node_id;
            std::string result;
            std::cin >> node_id;
            if (child_pid == 0) {
                child_pid = fork();
                if (child_pid == -1) {
                    std::cout << "Unable to create first worker node\n";
                    child_pid = 0;
                    exit(1);
                } else if (child_pid == 0) {
                    create_node(node_id, port);
                } else {
                    child_id = node_id;
                    send_message(main_socket,"pid");
                    result = recieve_message(main_socket);

                }

            } else {
//                if (child_id == node_id) {
//                    std::cout << "Error: Already exists";
//                }
                std::ostringstream msg_stream;
                msg_stream << "create " << node_id;
                send_message(main_socket, msg_stream.str());
                result = recieve_message(main_socket);
            }

            if (result.substr(0,2) == "Ok") {
                head = AddElement(head,node_id);
            }
            std::cout << result << "\n";

        } else if (command == "remove") {
            if (child_pid == 0) {
                std::cout << "Error:Not found\n";
                continue;
            }
            size_t node_id;
            std::cin >> node_id;
            if (node_id == child_id) {
                kill(child_pid, SIGTERM);
                kill(child_pid, SIGKILL);
                child_id = 0;
                child_pid = 0;
                std::cout << "Ok\n";
                head = RemoveElement(head,node_id);
                continue;
            }
            std::string message_string = "remove " + std::to_string(node_id);
            send_message(main_socket, message_string);
            std::string recieved_message = recieve_message(main_socket);
            if (recieved_message.substr(0, std::min<int>(recieved_message.size(), 2)) == "Ok") {
                head = RemoveElement(head,node_id);
            }
            std::cout << recieved_message << "\n";

        } else if (command == "exec") {
            int id, n;
            std::cin >> id >> n;
            std::vector<int> numbers(n);
            for (int i = 0; i < n; ++i) {
                std::cin >> numbers[i];
            }

            std::string message_string = "exec " + std::to_string(id) + " " + std::to_string(n);
            for (int i = 0; i < n; ++i) {
                message_string += " " + std::to_string(numbers[i]);
            }

            send_message(main_socket, message_string);
            std::string recieved_message = recieve_message(main_socket);
            std::cout << recieved_message << "\n";

        } else if (command == "pingall") {
            send_message(main_socket,"pingall");
            std::string recieved = recieve_message(main_socket);
            std::istringstream is;
            if (recieved.substr(0,std::min<int>(recieved.size(), 5)) == "Error") {
                is = std::istringstream("");
            } else {
                is = std::istringstream(recieved);
            }
            
            std::set<int> recieved_ids;
            int rec_id;
            while (is >> rec_id) {
                recieved_ids.insert(rec_id);
            }
            std::vector from_tree = get_nodes(head);
            auto part_it = std::partition(from_tree.begin(), from_tree.end(), [&recieved_ids] (int a) {
                return recieved_ids.count(a) == 0;
            });
            if (part_it == from_tree.begin()) {
                std::cout << "Ok: -1\n";
            } else {
                std::cout << "Ok:";
                for (auto it = from_tree.begin(); it != part_it; ++it) {
                    std::cout << " " << *it;
                }
                std::cout << "\n";
            }

        } else if (command == "exit") {
            break;
        }

    }

    return 0;
}