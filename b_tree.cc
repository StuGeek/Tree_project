#include "b_tree.h"
#include <semaphore.h>

// -----------------------------------------------------------------------------
//  BTree: b-tree to index hash values produced by qalsh
// -----------------------------------------------------------------------------
BTree::BTree()  // default constructor
{
    root_ = -1;
    file_ = NULL;
    root_ptr_ = NULL;
}

// -----------------------------------------------------------------------------
BTree::~BTree()  // destructor
{
    char *header = new char[file_->get_blocklength()];
    write_header(header);       // write <root_> to <header>
    file_->set_header(header);  // write back to disk
    delete[] header;
    header = NULL;

    if (root_ptr_ != NULL) {
        delete root_ptr_;
        root_ptr_ = NULL;
    }
    if (file_ != NULL) {
        delete file_;
        file_ = NULL;
    }
}

// -----------------------------------------------------------------------------
void BTree::init(       // init a new tree
    int b_length,       // block length
    const char *fname)  // file name
{
    FILE *fp = fopen(fname, "r");
    if (fp) {        // check whether the file exist
        fclose(fp);  // ask whether replace?
        // printf("The file \"%s\" exists. Replace? (y/n)", fname);

        // char c = getchar();			// input 'Y' or 'y' or others
        // getchar();					// input <ENTER>
        // assert(c == 'y' || c == 'Y');
        remove(fname);  // otherwise, remove existing file
    }
    file_ = new BlockFile(b_length, fname);  // b-tree stores here

    // -------------------------------------------------------------------------
    //  init the first node: to store <blocklength> (page size of a node),
    //  <number> (number of nodes including both index node and leaf node),
    //  and <root> (address of root node)
    // -------------------------------------------------------------------------
    root_ptr_ = new BIndexNode();
    root_ptr_->init(0, this);
    //返回BIndexNode中的变量block_
    root_ = root_ptr_->get_block();
    //释放root-ptr的内存
    delete_root();
}

// -----------------------------------------------------------------------------
void BTree::init_restore(  // load the tree from a tree file
    const char *fname)     // file name
{
    FILE *fp = fopen(fname, "r");  // check whether the file exists
    if (!fp) {
        printf("tree file %s does not exist\n", fname);
        exit(1);
    }
    fclose(fp);

    // -------------------------------------------------------------------------
    //  it doesn't matter to initialize blocklength to 0.
    //  after reading file, <blocklength> will be reinitialized by file.
    // -------------------------------------------------------------------------
    file_ = new BlockFile(0, fname);
    root_ptr_ = NULL;

    // -------------------------------------------------------------------------
    //  read the content after first 8 bytes of first block into <header>
    // -------------------------------------------------------------------------
    char *header = new char[file_->get_blocklength()];
    file_->read_header(header);  // read remain bytes from header
    read_header(header);         // init <root> from <header>

    delete[] header;
    header = NULL;
}

// -----------------------------------------------------------------------------
int BTree::bulkload(      // bulkload a tree from memory
    int n,                // number of entries
    const Result *table)  // hash table
{
    BIndexNode *index_child = NULL;
    BIndexNode *index_prev_nd = NULL;
    BIndexNode *index_act_nd = NULL;
    BLeafNode *leaf_child = NULL;
    BLeafNode *leaf_prev_nd = NULL;
    BLeafNode *leaf_act_nd = NULL;

    int id = -1;
    int block = -1;
    float key = MINREAL;

    // -------------------------------------------------------------------------
    //  build leaf node from <_hashtable> (level = 0)
    // -------------------------------------------------------------------------
    bool first_node = true;  // determine relationship of sibling
    int start_block = 0;     // position of first node
    int end_block = 0;       // position of last node

    for (int i = 0; i < n; ++i) {
        id = table[i].id_;
        key = table[i].key_;

        if (!leaf_act_nd) {
            leaf_act_nd = new BLeafNode();
            leaf_act_nd->init(0, this);

            if (first_node) {
                first_node = false;  // init <start_block>
                start_block = leaf_act_nd->get_block();
            } else {  // label sibling
                leaf_act_nd->set_left_sibling(leaf_prev_nd->get_block());
                leaf_prev_nd->set_right_sibling(leaf_act_nd->get_block());

                delete leaf_prev_nd;
                leaf_prev_nd = NULL;
            }
            end_block = leaf_act_nd->get_block();
        }
        leaf_act_nd->add_new_child(id, key);  // add new entry

        if (leaf_act_nd->isFull()) {  // change next node to store entries
            leaf_prev_nd = leaf_act_nd;
            leaf_act_nd = NULL;
        }
    }
    if (leaf_prev_nd != NULL) {
        delete leaf_prev_nd;
        leaf_prev_nd = NULL;
    }
    if (leaf_act_nd != NULL) {
        delete leaf_act_nd;
        leaf_act_nd = NULL;
    }

    // -------------------------------------------------------------------------
    //  stop condition: lastEndBlock == lastStartBlock (only one node, as root)
    // -------------------------------------------------------------------------
    int current_level = 1;               // current level (leaf level is 0)
    int last_start_block = start_block;  // build b-tree level by level
    int last_end_block = end_block;      // build b-tree level by level

    while (last_end_block > last_start_block) {
        first_node = true;
        for (int i = last_start_block; i <= last_end_block; ++i) {
            block = i;  // get <block>
            if (current_level == 1) {
                leaf_child = new BLeafNode();
                leaf_child->init_restore(this, block);
                key = leaf_child->get_key_of_node();

                delete leaf_child;
                leaf_child = NULL;
            } else {
                index_child = new BIndexNode();
                index_child->init_restore(this, block);
                key = index_child->get_key_of_node();

                delete index_child;
                index_child = NULL;
            }

            if (!index_act_nd) {
                index_act_nd = new BIndexNode();
                index_act_nd->init(current_level, this);

                if (first_node) {
                    first_node = false;
                    start_block = index_act_nd->get_block();
                } else {
                    index_act_nd->set_left_sibling(index_prev_nd->get_block());
                    index_prev_nd->set_right_sibling(index_act_nd->get_block());

                    delete index_prev_nd;
                    index_prev_nd = NULL;
                }
                end_block = index_act_nd->get_block();
            }
            index_act_nd->add_new_child(key, block);  // add new entry

            if (index_act_nd->isFull()) {
                index_prev_nd = index_act_nd;
                index_act_nd = NULL;
            }
        }
        if (index_prev_nd != NULL) {  // release the space
            delete index_prev_nd;
            index_prev_nd = NULL;
        }
        if (index_act_nd != NULL) {
            delete index_act_nd;
            index_act_nd = NULL;
        }

        last_start_block = start_block;  // update info
        last_end_block = end_block;      // build b-tree of higher level
        ++current_level;
    }
    root_ = last_start_block;  // update the <root>

    if (index_prev_nd != NULL) delete index_prev_nd;
    if (index_act_nd != NULL) delete index_act_nd;
    if (index_child != NULL) delete index_child;
    if (leaf_prev_nd != NULL) delete leaf_prev_nd;
    if (leaf_act_nd != NULL) delete leaf_act_nd;
    if (leaf_child != NULL) delete leaf_child;

    return 0;
}

// -----------------------------------------------------------------------------
void BTree::load_root()  // load root of b-tree
{
    if (root_ptr_ == NULL) {
        root_ptr_ = new BIndexNode();
        root_ptr_->init_restore(this, root_);
    }
}

// -----------------------------------------------------------------------------
void BTree::delete_root()  // delete root of b-tree
{
    if (root_ptr_ != NULL) {
        delete root_ptr_;
        root_ptr_ = NULL;
    }
}

// -----------------------------------------------------------------------------
// 并行程序设计部分

#define THREADS_NUM 1000           // 线程池中的线程个数
#define TASK_QUEUE_MAX_SIZE 50000  // 任务的等待队列的最大长度，等待队列中的最大任务个数为长度减一

#define THREAD_ADD_CHILD_NUM 5     // 每个线程在添加条目或儿子结点时一次添加的最多个数

// 任务完成数变量的锁
pthread_mutex_t task_num_finish_mutex = PTHREAD_MUTEX_INITIALIZER;
// 调用结点的init_restore方法时使用的锁
pthread_mutex_t init_restore_mutex = PTHREAD_MUTEX_INITIALIZER;

// 初始化叶子结点所使用的参数结构体
struct Init_Leaf_Node_Params {
    BLeafNode *first_leaf_node;      // 叶子结点层的第一个结点，如果这个初始化的叶子结点不是第一个结点，这个参数为NULL
    BTree *btree;                    // 叶子结点所在B+树
    int n;                           // 总的条目个数
    const Result *table;             // 存储条目信息的哈希表
    int leaf_node_capacity_entries;  // 叶子结点可以存储的最大条目数
    int leaf_node_capacity_keys;     // 叶子结点可以存储的最大关键字数
    int leaf_node_num;               // 叶子结点的总数
};

// 初始化索引结点所使用的参数结构体
struct Init_Index_Node_Params {
    BIndexNode *first_index_node;     // 这个索引结点所在层的第一个结点，如果这个初始化的索引结点不是该层第一个结点，这个参数为NULL
    BTree *btree;                     // 叶子结点所在B+树
    int last_start_block;             // 索引结点下一层结点的开头结点的block位置
    int last_end_block;               // 索引结点下一层结点的结尾结点的block位置
    int current_level;                // 索引结点所在层数
    int index_node_capacity_entries;  // 索引结点可以存储的最大儿子结点数
    int index_node_num_cur_level;     // 索引结点所处层的索引结点数
};

// 并行向叶子结点添加条目所使用的参数结构体
struct Add_Leaf_Node_Child_Params {
    BLeafNode *leaf_node;  // 所要添加条目的叶子结点
    int begin_index;       // 每次添加进叶子结点的id数组的起始条目在哈希表中的下标位置
    int end_index;         // 每次添加进叶子结点的id数组的末尾条目在哈希表中的下标位置
    const Result *table;   // 存储条目信息的哈希表
    int start_pos;         // 添加进叶子结点的id数组的第一个条目在哈希表中的下标位置
};

// 并行向索引结点添加关键字和儿子结点的位置所使用的参数结构体
struct Add_Index_Node_Child_Params {
    BIndexNode *index_node;  // 所要添加儿子结点的索引结点
    int begin_child_block;   // 每次添加的起始儿子结点的block位置
    int end_child_block;     // 每次添加的末尾儿子结点的block位置
    int start_pos;           // 添加的儿子结点的block的起始位置
    int cur_level;           // 索引结点层数
    BTree *btree;            // 索引结点所在的B+树
};

// 线程池中每个线程执行的任务的结构体
typedef struct {
    void *(*function)(void *);  // 执行函数
    void *arg;                  // 参数
} Task;

// 任务循环队列的数据结构
typedef struct {
    Task tasks[TASK_QUEUE_MAX_SIZE];  // 任务队列数组
    int front;                        // 队首下标
    int rear;                         // 队尾下标
} TaskQueue;

// 线程池数据结构
typedef struct {
    pthread_t threads[THREADS_NUM];  // 线程数组
    TaskQueue taskQueue;             // 任务队列
    bool isEnd;                      // 程序是否结束，要销毁线程池
    sem_t sem_mutex;                 // 互斥信号量
} Threadpools;

Threadpools pools;                  // 线程池
long long int task_num_finish = 0;  // 完成的任务数
long long int task_num = 0;         // 要完成的任务数

// 线程池中每个线程执行的任务
static void *executeTask(void *arg) {
    // 向每个线程传入的参数是线程池
    Threadpools *pools = (Threadpools *)arg;
    while (1) {
        // 等待互斥信号量大于0，防止临界冲突，然后将互斥锁信号量减一，继续执行
        sem_wait(&pools->sem_mutex);
        // 当任务队列为空时
        while (pools->taskQueue.front == pools->taskQueue.rear) {
            // 如果已经没有剩余任务要处理，那么退出线程
            if (pools->isEnd) {
                sem_post(&pools->sem_mutex);
                pthread_exit(NULL);
            }
            // 否则等待任务队列中有任务后再取任务进行执行
            sleep(0);
        }
        // 获取任务队列队首的任务
        Task task;
        int front = pools->taskQueue.front;
        task.function = pools->taskQueue.tasks[front].function;
        task.arg = pools->taskQueue.tasks[front].arg;
        // 循环队列队首下标加一
        pools->taskQueue.front = (front + 1) % TASK_QUEUE_MAX_SIZE;

        // 将互斥锁信号量加一，允许其它线程执行
        sem_post(&pools->sem_mutex);

        // 执行任务
        (*(task.function))(task.arg);
    }
}

// 初始化线程池
void initThreadpools(Threadpools *pools) {
    int ret;
    // 任务队列的队首和队尾的坐标都为0
    pools->taskQueue.front = 0;
    pools->taskQueue.rear = 0;
    pools->isEnd = false;

    // 初始化互斥信号量为0
    ret = sem_init(&pools->sem_mutex, 0, 1);
    if (ret == -1) {
        perror("sem_init-mutex");
        exit(1);
    }

    // 创建线程池中的线程
    for (int i = 0; i < THREADS_NUM; ++i) {
        ret = pthread_create(&pools->threads[i], NULL, executeTask, (void *)pools);
        if (ret != 0) {
            fprintf(stderr, "pthread_create error: %s\n", strerror(ret));
            exit(1);
        }
    }
}

// 向任务队列中添加任务
void addTask(Threadpools *pools, void *(*function)(void *arg), void *arg) {
    // 当任务队列为满时，等待有任务被取出任务队列不为满再加入队列
    while ((pools->taskQueue.rear + TASK_QUEUE_MAX_SIZE + 1 -
              pools->taskQueue.front) % TASK_QUEUE_MAX_SIZE == 0) {
        sleep(0);
    }
    // 向任务队列的队尾加入任务
    Task task;
    task.function = function;
    task.arg = arg;
    int rear = pools->taskQueue.rear;
    pools->taskQueue.tasks[rear] = task;
    // 任务队列队尾下标加一
    pools->taskQueue.rear = (rear + 1) % (TASK_QUEUE_MAX_SIZE);
}

// 向叶子结点添加条目所使用的线程函数
static void *add_leaf_node_child(void *arg) {
    // 获取参数
    Add_Leaf_Node_Child_Params *params = (Add_Leaf_Node_Child_Params *)arg;

    BLeafNode *leaf_node = params->leaf_node;
    int begin_index = params->begin_index;
    int end_index = params->end_index;
    const Result *table = params->table;
    int start_pos = params->start_pos;

    // 一次添加end_index - begin_index + 1个条目
    for (int i = begin_index; i <= end_index; ++i) {
        // 向叶子结点的id_数组的相应下标位置添加条目
        leaf_node->set_id(table[i].id_, i - start_pos);
        // 如果满足一定的条件，向叶子结点的key_数组的相应下标位置添加关键字
        if (((i - start_pos) * SIZEINT) % LEAF_NODE_SIZE == 0) {
            leaf_node->set_key(table[i].key_, ((i - start_pos) * SIZEINT) / LEAF_NODE_SIZE);
        }
    }

    sleep(0);

    // 添加完成的任务数
    pthread_mutex_lock(&task_num_finish_mutex);
    task_num_finish += end_index - begin_index + 1;
    pthread_mutex_unlock(&task_num_finish_mutex);
}

// 向索引结点添加关键字和儿子结点的位置所使用的线程函数
static void *add_index_node_child(void *arg) {
    // 获取参数
    Add_Index_Node_Child_Params *params = (Add_Index_Node_Child_Params *)arg;

    BIndexNode *index_node = params->index_node;
    int begin_child_block = params->begin_child_block;
    int end_child_block = params->end_child_block;
    int start_pos = params->start_pos;
    int cur_level = params->cur_level;
    BTree *btree = params->btree;

    float key;
    if (cur_level == 1) {
        // 一次添加end_child_block - begin_child_block + 1个儿子结点
        for (int i = begin_child_block; i <= end_child_block; ++i) {
            BLeafNode *leaf_child = new BLeafNode();
            pthread_mutex_lock(&init_restore_mutex);
            leaf_child->init_restore(btree, i);
            pthread_mutex_unlock(&init_restore_mutex);
            // 获取这个结点的关键字
            key = leaf_child->get_key_of_node();

            if (leaf_child != NULL) {
                delete leaf_child;
                leaf_child = NULL;
            }

            // 向索引结点添加关键字和儿子结点
            index_node->set_key(key, i - start_pos);
            index_node->set_son(i, i - start_pos);
        }
    } else {
        // 一次添加end_child_block - begin_child_block + 1个儿子结点
        for (int i = begin_child_block; i <= end_child_block; ++i) {
            BIndexNode *index_child = new BIndexNode();
            pthread_mutex_lock(&init_restore_mutex);
            index_child->init_restore(btree, i);
            pthread_mutex_unlock(&init_restore_mutex);
            // 获取这个结点的关键字
            key = index_child->get_key_of_node();

            if (index_child != NULL) {
                delete index_child;
                index_child = NULL;
            }

            // 向索引结点添加关键字和儿子结点
            index_node->set_key(key, i - start_pos);
            index_node->set_son(i, i - start_pos);
        }
    }

    sleep(0);

    // 添加完成的任务数
    pthread_mutex_lock(&task_num_finish_mutex);
    task_num_finish += end_child_block - begin_child_block + 1;;
    pthread_mutex_unlock(&task_num_finish_mutex);
}

// 初始化叶子结点
static void init_the_leaf_node(void *arg) {
    // 获取参数
    Init_Leaf_Node_Params *init_params = (Init_Leaf_Node_Params *)arg;

    BLeafNode *first_leaf_node = init_params->first_leaf_node;
    BTree *btree = init_params->btree;
    int n = init_params->n;
    const Result *table = init_params->table;
    int leaf_node_capacity_entries = init_params->leaf_node_capacity_entries;
    int leaf_node_capacity_keys = init_params->leaf_node_capacity_keys;
    int leaf_node_num = init_params->leaf_node_num;

    int i, ret;

    // 声明初始化的叶子结点
    BLeafNode *leaf_node;
    // 如果作为参数传递进来的first_leaf_node为空
    if (first_leaf_node == NULL) {
        // 说明初始化的这个结点不是该层的第一个结点，调用init方法进行初始化
        leaf_node = new BLeafNode();
        // 调用init方法时，会改变block的位置序号，每次调用位置block加一，所以要加锁
        leaf_node->init(0, btree);
    }
    // first_leaf_node不为空说明是第一个结点，直接获取结点
    else {
        leaf_node = first_leaf_node;
    }

    // 获取结点位置
    int block = leaf_node->get_block();

    // 向叶子结点添加条目在哈希表中的起始位置
    int start_pos = (block - 1) * leaf_node_capacity_entries;
    // 向叶子结点添加条目在哈希表中的终点位置
    int end_pos = start_pos + leaf_node_capacity_entries;
    // 如果终点位置大于条目个数，那么终点位置直接为条目的末尾n
    if (end_pos > n) {
        end_pos = n;
    }

    // 向这个叶子结点添加的总条目数
    int num_entries = end_pos - start_pos;
    // 要完成的任务数加上要添加的条目数
    task_num += num_entries;

    // 向这个叶子结点添加的关键字数
    int num_keys;
    if ((num_entries * SIZEINT) % LEAF_NODE_SIZE == 0) {
        num_keys = (num_entries * SIZEINT) / LEAF_NODE_SIZE;
    } else {
        num_keys = (num_entries * SIZEINT) / LEAF_NODE_SIZE + 1;
    }

    assert(num_keys <= leaf_node_capacity_keys);

    // 并行添加条目的参数数组
    Add_Leaf_Node_Child_Params params[num_entries];
    i = 0;
    while (i < num_entries) {
        // 在数组中赋值并传递参数，可保证参数地址固定，避免并行时参数出现错误
        params[i].leaf_node = leaf_node;
        params[i].begin_index = i + start_pos;
        // 确定添加进结点的末尾条目的位置
        if (i + THREAD_ADD_CHILD_NUM - 1 >= num_entries) {
            params[i].end_index = start_pos + num_entries - 1;
        }
        else {
            params[i].end_index = i + start_pos + THREAD_ADD_CHILD_NUM - 1;
        }
        params[i].table = table;
        params[i].start_pos = start_pos;

        // 将添加条目的线程加入任务队列等待执行
        addTask(&pools, add_leaf_node_child, (void *)&params[i]);

        i += THREAD_ADD_CHILD_NUM;
    }

    // 当完成的任务数小于要完成的任务数，等待完成
    while (task_num_finish < task_num) {
        sleep(0);
    }

    // 设置叶子结点含有的条目数、关键字数、是脏块删除指针要写回文件
    leaf_node->set_num_entries(num_entries);
    leaf_node->set_num_keys(num_keys);
    leaf_node->set_dirty(true);

    // 如果结点不是这层第一个结点，设置它的左兄弟是前一个结点
    if (block > 1) {
        leaf_node->set_left_sibling(block - 1);
    }

    // 如果结点不是此层最后一个结点，设置它的右兄弟是后一个结点
    if (block < leaf_node_num) {
        leaf_node->set_right_sibling(block + 1);
    }

    // 删除结点指针，使其信息写回文件
    if (leaf_node != NULL) {
        delete leaf_node;
        leaf_node = NULL;
    }
}

// 初始化索引结点
static void init_the_index_node(void *arg) {
    // 获取参数
    Init_Index_Node_Params *init_params = (Init_Index_Node_Params *)arg;

    BIndexNode *first_index_node = init_params->first_index_node;
    BTree *btree = init_params->btree;
    int last_start_block = init_params->last_start_block;
    int last_end_block = init_params->last_end_block;
    int current_level = init_params->current_level;
    int index_node_capacity_entries = init_params->index_node_capacity_entries;
    int index_node_num_cur_level = init_params->index_node_num_cur_level;

    int i, ret;

    // 声明初始化的索引结点
    BIndexNode *index_node;
    // 如果作为参数传递进来的first_index_node为空
    if (first_index_node == NULL) {
        // 说明初始化的这个结点不是该层的第一个结点，调用init方法进行初始化
        index_node = new BIndexNode();
        index_node->init(current_level, btree);
    }
    // first_index_node不为空说明是第一个结点，直接获取结点
    else {
        index_node = first_index_node;
    }

    // 获取结点位置
    int block = index_node->get_block();

    // 索引结点相对这一层的第一个结点的位置
    int block_index_cur_level = block - last_end_block - 1;

    // 向索引结点添加的儿子结点的block的起始位置
    int start_pos = last_start_block + index_node_capacity_entries * block_index_cur_level;
    // 向索引结点添加的儿子结点的block的终点位置
    int end_pos = start_pos + index_node_capacity_entries - 1;
    // 如果终点位置大于下一层结点个数，那么终点位置直接为下一层末尾结点位置last_end_block
    if (end_pos > last_end_block) {
        end_pos = last_end_block;
    }

    // 向索引结点添加的儿子结点的总数
    int num_entries = end_pos - start_pos + 1;
    // 要完成的任务数加上要添加的儿子结点数
    task_num += num_entries;

    // 添加儿子结点的参数数组
    Add_Index_Node_Child_Params params[num_entries];

    i = 0;
    while (i < num_entries) {
        // 在数组中赋值并传递参数，可保证参数地址固定，避免并行时参数出现错误
        params[i].index_node = index_node;
        params[i].begin_child_block = i + start_pos;
        // 确定添加进结点的末尾儿子结点的位置
        if (i + THREAD_ADD_CHILD_NUM - 1 >= num_entries) {
            params[i].end_child_block = start_pos + num_entries - 1;
        }
        else {
            params[i].end_child_block = i + start_pos + THREAD_ADD_CHILD_NUM - 1;
        }
        params[i].start_pos = start_pos;
        params[i].cur_level = current_level;
        params[i].btree = btree;

        // 将添加儿子结点的线程加入任务队列等待执行
        addTask(&pools, add_index_node_child, (void *)&params[i]);

        i += THREAD_ADD_CHILD_NUM;
    }

    // 当完成的任务数小于要完成的任务数，等待完成
    while (task_num_finish < task_num) {
        sleep(0);
    }

    // 设置所以结点含有的儿子结点数、是脏块删除指针要写回文件
    index_node->set_num_entries(num_entries);
    index_node->set_dirty(true);

    // 如果结点不是这层第一个结点，设置它的左兄弟是前一个结点
    if (block_index_cur_level > 0) {
        index_node->set_left_sibling(block - 1);
    }

    // 如果结点不是此层最后一个结点，设置它的右兄弟是后一个结点
    if (block_index_cur_level < index_node_num_cur_level - 1) {
        index_node->set_right_sibling(block + 1);
    }

    // 删除结点指针，使其信息写回文件
    if (index_node != NULL) {
        delete index_node;
        index_node = NULL;
    }
}

// -----------------------------------------------------------------------------
int BTree::bulkload_parallel(  // parallel bulkload a tree from memory
    int n,                     // number of entries
    const Result *table)       // hash table
{
    int i, ret;

    // 创建并初始化线程池
    // Threadpools pools;
    initThreadpools(&pools);

    printf("\nthread number: %d\n", THREADS_NUM);
    printf("one thread add child num: %d\n", THREAD_ADD_CHILD_NUM);

    // -----------------------------------------------------------------------------
    // 加载叶子结点

    // -------------------------------------------------------------------------
    //  build leaf node from <_hashtable> (level = 0)
    // -------------------------------------------------------------------------
    int start_block = 0;  // position of first node
    int end_block = 0;    // position of last node

    // 创建第一个叶子结点
    BLeafNode *first_leaf_node = new BLeafNode();
    // 进行初始化
    first_leaf_node->init(0, this);
    // 获取第一个叶子结点的位置作为开头位置
    start_block = first_leaf_node->get_block();

    // 通过创建的叶子结点，获取每个叶子结点可以添加的最大条目数和最大关键字数
    int leaf_node_capacity_entries = first_leaf_node->get_capacity();
    int leaf_node_capacity_keys = first_leaf_node->get_capacity_keys();

    // 通过最大条目数，计算要创建的叶子结点数
    int leaf_node_num;
    if (n % leaf_node_capacity_entries == 0) {
        leaf_node_num = n / leaf_node_capacity_entries;
    } else {
        leaf_node_num = n / leaf_node_capacity_entries + 1;
    }

    // 初始化叶子结点的参数数组
    Init_Leaf_Node_Params init_leaf_node_params[leaf_node_num];
    for (i = 0; i < leaf_node_num; i++) {
        // 第一个初始化的叶子结点，将之前创建的第一个叶子结点直接作为参数传递进去
        if (i == 0) {
            init_leaf_node_params[i].first_leaf_node = first_leaf_node;
        }
        // 其它结点传递NULL
        else {
            init_leaf_node_params[i].first_leaf_node = NULL;
        }
        // 设置相应参数
        init_leaf_node_params[i].btree = this;
        init_leaf_node_params[i].table = table;
        init_leaf_node_params[i].n = n;
        init_leaf_node_params[i].leaf_node_capacity_entries = leaf_node_capacity_entries;
        init_leaf_node_params[i].leaf_node_capacity_keys = leaf_node_capacity_keys;
        init_leaf_node_params[i].leaf_node_num = leaf_node_num;

        // 对每个结点进行初始化
        init_the_leaf_node((void *)&init_leaf_node_params[i]);
    }

    // 叶子结点层的末尾位置为开始位置加上叶子结点个数减一
    end_block = start_block + leaf_node_num - 1;

    // -----------------------------------------------------------------------------
    // 加载索引结点

    // -------------------------------------------------------------------------
    //  stop condition: lastEndBlock == lastStartBlock (only one node, as root)
    // -------------------------------------------------------------------------
    int current_level = 1;               // current level (leaf level is 0)
    int last_start_block = start_block;  // build b-tree level by level
    int last_end_block = end_block;      // build b-tree level by level

    // 当之前层末尾位置大于之前层开头位置时，说明还没有插入到根结点（last_start_block == last_end_block）
    while (last_end_block > last_start_block) {
        // 开始构建每一层的索引结点，首先创建每一层的第一个索引结点
        BIndexNode *first_index_node = new BIndexNode();
        // 进行初始化
        first_index_node->init(current_level, this);
        // 获取每一层第一个索引结点的位置作为该层开头位置
        int block = first_index_node->get_block();
        start_block = block;

        // 通过创建的索引结点，获取每个索引结点可以添加的最大儿子结点数和最大关键字数
        int index_node_capacity_entries = first_index_node->get_capacity();

        // 计算下一层的结点数量
        int node_num_last_level = last_end_block - last_start_block + 1;
        // 根据下一层的结点数量和最大儿子结点数计算这一层的结点数量
        int index_node_num_cur_level;
        if (node_num_last_level % index_node_capacity_entries == 0) {
            index_node_num_cur_level = node_num_last_level / index_node_capacity_entries;
        } else {
            index_node_num_cur_level = node_num_last_level / index_node_capacity_entries + 1;
        }

        // 初始化索引结点的参数数组
        Init_Index_Node_Params init_index_node_params[index_node_num_cur_level];
        for (i = 0; i < index_node_num_cur_level; i++) {
            // 此层第一个初始化的索引结点，将之前创建的此层第一个索引结点直接作为参数传递进去
            if (i == 0) {
                init_index_node_params[i].first_index_node = first_index_node;
            }
            // 其它结点传递NULL
            else {
                init_index_node_params[i].first_index_node = NULL;
            }
            // 设置相应参数
            init_index_node_params[i].btree = this;
            init_index_node_params[i].last_start_block = last_start_block;
            init_index_node_params[i].last_end_block = last_end_block;
            init_index_node_params[i].current_level = current_level;
            init_index_node_params[i].index_node_capacity_entries = index_node_capacity_entries;
            init_index_node_params[i].index_node_num_cur_level = index_node_num_cur_level;

            // 对每个结点进行初始化
            init_the_index_node((void *)&init_index_node_params[i]);
        }

        // 此层索引结点的末尾位置为开始位置加上此层索引结点个数减一
        end_block = start_block + index_node_num_cur_level - 1;

        last_start_block = start_block;  // update info
        last_end_block = end_block;      // build b-tree of higher level
        ++current_level;
    }

    // 根结点的位置即上一层的开头位置
    root_ = last_start_block;  // update the <root>

    // 销毁线程池
    pools.isEnd = true;

    // 主线程等待线程池中的线程全部结束后再继续
    for (int i = 0; i < THREADS_NUM; ++i) {
        ret = pthread_join(pools.threads[i], NULL);
        if (ret != 0) {
            fprintf(stderr, "pthread_join error: %s\n", strerror(ret));
            exit(1);
        }
    }

    pthread_mutex_destroy(&task_num_finish_mutex);
    pthread_mutex_destroy(&init_restore_mutex);

    // 销毁互斥信号量
    ret = sem_destroy(&pools.sem_mutex);
    if (ret == -1) {
        perror("sem_destroy sem_mutex");
    }

    return 0;
}
