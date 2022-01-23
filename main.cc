#include <iostream>
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <string>
#include <set>
#include <fstream>
#include <sstream>

#include "def.h"
#include "util.h"
#include "random.h"
#include "pri_queue.h"
#include "b_node.h"
#include "b_tree.h"

using namespace std;

void recursive_print(BTree *btree, int level, int block, string space) {
	if (level == 0) {
		BLeafNode *leaf_node = new BLeafNode();
		leaf_node->init_restore(btree, block);
		int key_index = 0;
		int num_entries = leaf_node->get_num_entries();
		printf("%sblock: %d (leaf node)\n", space.c_str(), block);
		for (int i = 0; i < num_entries; ++i) {
			if ((i * SIZEINT) % LEAF_NODE_SIZE == 0) {
				printf("%s  ", space.c_str());
				int key = leaf_node->get_key(key_index);
				printf("key:%d\n", key);
				key_index++;
			}
			printf("%s    ", space.c_str());
			printf("id:%d\n", leaf_node->get_entry_id(i));
		}
		delete leaf_node;
		leaf_node = NULL;
	}
	else {
		BIndexNode *index_node = new BIndexNode();
		index_node->init_restore(btree, block);
		int num_entries = index_node->get_num_entries();
		printf("%sblock: %d (index node)\n", space.c_str(), block);
		for (int i = 0; i < num_entries; ++i) {
			printf("%s  ", space.c_str());
			int key = index_node->get_key(i);
			printf("key:%d son_block:%d\n", key, index_node->get_son(i));
			recursive_print(btree, level - 1, index_node->get_son(i), space + "  ");
		}
		delete index_node;
		index_node = NULL;
	}
}

void print_tree(BTree *btree) {
	int root_block = btree->root_;
	BNode *root_ptr;
	if (root_block == 1) {
		root_ptr = new BLeafNode();
	}
	else {
		root_ptr = new BIndexNode();
	}
	root_ptr->init_restore(btree, root_block);
	int level = root_ptr->get_level();
	recursive_print(btree, level, root_block, "");

	delete root_ptr;
	root_ptr = NULL;
}

// -----------------------------------------------------------------------------
int main(int nargs, char **args)
{    

	char data_file[200];
	char tree_file[200];
	int  B_ = 512; // node size
	// int n_pts_ = 1000000;
	int n_pts_ = 1000000;

	strncpy(data_file, "./data/dataset.csv", sizeof(data_file));
	strncpy(tree_file, "./result/B_tree", sizeof(tree_file));
	printf("data_file   = %s\n", data_file);
	printf("tree_file   = %s\n", tree_file);
	printf("\nn_pts: %d\n", n_pts_);

	Result *table = new Result[n_pts_]; 

	ifstream fp(data_file); 
	string line;
	int i=0;
	while (getline(fp,line)){ 
        string number;
        istringstream readstr(line); 
        
		getline(readstr,number,','); 
		table[i].key_ = atof(number.c_str()); 

		getline(readstr,number,','); 
		table[i].id_ = atoi(number.c_str());    
        i++;
		if (i == n_pts_) break;
    }
	
	fp.close();

	timeval start_t;  
    timeval end_t;

	gettimeofday(&start_t,NULL);
		
	BTree* trees_ = new BTree();
	trees_->init(B_, tree_file);
	
	if (nargs == 1) {
		if (trees_->bulkload(n_pts_, table)) return 1;
	}
	else if (nargs > 1) {
		if (!strcmp(args[1], "print")) {
			if (trees_->bulkload(n_pts_, table)) return 1;
			print_tree(trees_);
		}
		if (!strcmp(args[1], "parallel")) {
			if (trees_->bulkload_parallel(n_pts_, table)) return 1;
			if (nargs > 2 && !strcmp(args[2], "print")) {
				print_tree(trees_);
			}
		}
	}

	//对这个函数进行并行
	// if (trees_->bulkload_parallel(n_pts_, table)) return 1;
	// print_tree(trees_);

	delete[] table; table = NULL;

	gettimeofday(&end_t, NULL);

	float run_t1 = end_t.tv_sec - start_t.tv_sec + 
						(end_t.tv_usec - start_t.tv_usec) / 1000000.0f;
	printf("运行时间: %f  s\n", run_t1);
	

	return 0;
}
