#include <assert.h>
#include "btree.h"

#define RETURNIFERROR(rc) if(rc) {return rc;}

KeyValuePair::KeyValuePair()
{}


KeyValuePair::KeyValuePair(const KEY_T &k, const VALUE_T &v) : 
  key(k), value(v)
{}


KeyValuePair::KeyValuePair(const KeyValuePair &rhs) :
  key(rhs.key), value(rhs.value)
{}


KeyValuePair::~KeyValuePair()
{
}


KeyValuePair & KeyValuePair::operator=(const KeyValuePair &rhs)
{
  return *( new (this) KeyValuePair(rhs));
}

BTreeIndex::BTreeIndex(SIZE_T keysize, 
		       SIZE_T valuesize,
		       BufferCache *cache,
		       bool unique) 
{
  superblock.info.keysize=keysize;
  superblock.info.valuesize=valuesize;
  buffercache=cache;
  // note: ignoring unique now
}

BTreeIndex::BTreeIndex()
{
  // shouldn't have to do anything
}


//
// Note, will not attach!
//
BTreeIndex::BTreeIndex(const BTreeIndex &rhs)
{
  buffercache=rhs.buffercache;
  superblock_index=rhs.superblock_index;
  superblock=rhs.superblock;
}

BTreeIndex::~BTreeIndex()
{
  // shouldn't have to do anything
}


BTreeIndex & BTreeIndex::operator=(const BTreeIndex &rhs)
{
  return *(new(this)BTreeIndex(rhs));
}


ERROR_T BTreeIndex::AllocateNode(SIZE_T &n)
{
  n=superblock.info.freelist;

  if (n==0) { 
    return ERROR_NOSPACE;
  }

  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype==BTREE_UNALLOCATED_BLOCK);

  superblock.info.freelist=node.info.freelist;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyAllocateBlock(n);

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::DeallocateNode(const SIZE_T &n)
{
  BTreeNode node;

  node.Unserialize(buffercache,n);

  assert(node.info.nodetype!=BTREE_UNALLOCATED_BLOCK);

  node.info.nodetype=BTREE_UNALLOCATED_BLOCK;

  node.info.freelist=superblock.info.freelist;

  node.Serialize(buffercache,n);

  superblock.info.freelist=n;

  superblock.Serialize(buffercache,superblock_index);

  buffercache->NotifyDeallocateBlock(n);

  return ERROR_NOERROR;

}

ERROR_T BTreeIndex::Attach(const SIZE_T initblock, const bool create)
{
  ERROR_T rc;

  superblock_index=initblock;
  assert(superblock_index==0);

  if (create) {
    // build a super block, root node, and a free space list
    //
    // Superblock at superblock_index
    // root node at superblock_index+1
    // free space list for rest
    BTreeNode newsuperblock(BTREE_SUPERBLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
    newsuperblock.info.rootnode=superblock_index+1;
    newsuperblock.info.freelist=superblock_index+2;
    newsuperblock.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index);

    rc=newsuperblock.Serialize(buffercache,superblock_index);

    if (rc) { 
      return rc;
    }
    
    BTreeNode newrootnode(BTREE_ROOT_NODE,
			  superblock.info.keysize,
			  superblock.info.valuesize,
			  buffercache->GetBlockSize());
    newrootnode.info.rootnode=superblock_index+1;
    newrootnode.info.freelist=superblock_index+2;
    newrootnode.info.numkeys=0;

    buffercache->NotifyAllocateBlock(superblock_index+1);

    rc=newrootnode.Serialize(buffercache,superblock_index+1);

    if (rc) { 
      return rc;
    }

    for (SIZE_T i=superblock_index+2; i<buffercache->GetNumBlocks();i++) { 
      BTreeNode newfreenode(BTREE_UNALLOCATED_BLOCK,
			    superblock.info.keysize,
			    superblock.info.valuesize,
			    buffercache->GetBlockSize());
      newfreenode.info.rootnode=superblock_index+1;
      newfreenode.info.freelist= ((i+1)==buffercache->GetNumBlocks()) ? 0: i+1;
      
      rc = newfreenode.Serialize(buffercache,i);

      if (rc) {
	return rc;
      }

    }
  }

  // OK, now, mounting the btree is simply a matter of reading the superblock 

  return superblock.Unserialize(buffercache,initblock);
}
    

ERROR_T BTreeIndex::Detach(SIZE_T &initblock)
{
  return superblock.Serialize(buffercache,superblock_index);
}
 

ERROR_T BTreeIndex::LookupOrUpdateInternal(const SIZE_T &node,
					   const BTreeOp op,
					   const KEY_T &key,
					   VALUE_T &value)
{
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;
  KEY_T testkey;
  SIZE_T ptr;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
      if (b.info.numkeys == 0) {
          rc = b.GetPtr(0, ptr);
          RETURNIFERROR(rc)
          return LookupOrUpdateInternal(ptr, op, key, value);
      }
  case BTREE_INTERIOR_NODE:
    // Scan through key/ptr pairs
    //and recurse if possible
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (key<testkey) {
	// OK, so we now have the first key that's larger
	// so we ned to recurse on the ptr immediately previous to 
	// this one, if it exists
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	return LookupOrUpdateInternal(ptr,op,key,value);
      }
    }
    // if we got here, we need to go to the next pointer, if it exists
    if (b.info.numkeys>0) { 
      rc=b.GetPtr(b.info.numkeys,ptr);
      if (rc) { return rc; }
      return LookupOrUpdateInternal(ptr,op,key,value);
    } else {
      // There are no keys at all on this node, so nowhere to go
      return ERROR_NONEXISTENT;
    }
    break;
  case BTREE_LEAF_NODE:
    // Scan through keys looking for matching value
    for (offset=0;offset<b.info.numkeys;offset++) { 
      rc=b.GetKey(offset,testkey);
      if (rc) {  return rc; }
      if (testkey==key) { 
	    if (op==BTREE_OP_LOOKUP) {
	        return b.GetVal(offset,value);
	    } else if (op==BTREE_OP_UPDATE) {
	        // BTREE_OP_UPDATE
            rc = b.SetVal(offset, value);
            RETURNIFERROR(rc)
            return b.Serialize(buffercache, node);

	    } else {
            return ERROR_INSANE;
        }
      }
    }
    return ERROR_NONEXISTENT;
  default:
    // We can't be looking at anything other than a root, internal, or leaf
    return ERROR_INSANE;
    break;
  }  

  return ERROR_INSANE;
}

ERROR_T BTreeIndex::InsertKeyPtr(SIZE_T &node, KEY_T &key, SIZE_T &ptr) {
    BTreeNode b;
    ERROR_T rc;
    SIZE_T offset;
    SIZE_T temp;
    KEY_T testkey;
    SIZE_T tempptr;

    rc = b.Unserialize(buffercache, node);
    RETURNIFERROR(rc)

    switch (b.info.nodetype) {
        case BTREE_ROOT_NODE: {
            SIZE_T maxsize = b.info.GetNumSlotsAsInterior();
            // root node split special case
            if (b.info.numkeys >= maxsize) {
                SIZE_T leftptr;
                SIZE_T rightptr;

                rc = AllocateNode(leftptr);
                RETURNIFERROR(rc)
                rc = AllocateNode(rightptr);
                RETURNIFERROR(rc)

                BTreeNode leftnode(BTREE_INTERIOR_NODE,
                                   superblock.info.keysize,
                                   superblock.info.valuesize,
                                   superblock.info.blocksize);
                BTreeNode rightnode(BTREE_INTERIOR_NODE,
                                    superblock.info.keysize,
                                    superblock.info.valuesize,
                                    superblock.info.blocksize);
                rc = leftnode.Serialize(buffercache, leftptr);
                RETURNIFERROR(rc)
                rc = rightnode.Serialize(buffercache, rightptr);
                RETURNIFERROR(rc)

                SIZE_T halfsize = (maxsize + 1) / 2;
                SIZE_T movedcount = 0;
                SIZE_T insertmoved = 0;
                SIZE_T rootkeymoved = 0;
                SIZE_T splitnodeoffset = 0;
                KEY_T *keys = new KEY_T[maxsize+1];
                SIZE_T *ptrs = new SIZE_T[maxsize+1];
                for (offset=0; offset<b.info.numkeys; offset++) {
                    rc = b.GetKey(offset, *(keys+offset+insertmoved));
                    RETURNIFERROR(rc)
                    rc = b.GetPtr(offset+1, *(ptrs+offset+insertmoved));
                    RETURNIFERROR(rc)
                    if (keys[offset+insertmoved] > key && !insertmoved) {
                        *(keys+offset+insertmoved) = key;
                        *(ptrs+offset+insertmoved) = ptr;
                        insertmoved = 1;
                        rc = b.GetKey(offset, *(keys+offset+insertmoved));
                        RETURNIFERROR(rc)
                        rc = b.GetPtr(offset+1, *(ptrs+offset+insertmoved));
                        RETURNIFERROR(rc)
                    } else if (keys[offset+insertmoved] == key) {
                        return ERROR_CONFLICT;
                    }

                }
                if (!insertmoved) {
                    *(keys+offset+insertmoved) = key;
                    *(ptrs+offset+insertmoved) = ptr;
                }
                b.info.numkeys = 0;
                // set 0 ptr for left node
                rc = b.GetPtr(0, tempptr);
                RETURNIFERROR(rc)
                rc = leftnode.SetPtr(0, tempptr);
                RETURNIFERROR(rc)
                for (offset=0; offset<maxsize+1; offset++) {
                    if (movedcount < halfsize) {
                        leftnode.info.numkeys++;
                        rc = leftnode.SetKey(offset, *(keys+offset));
                        RETURNIFERROR(rc)
                        rc = leftnode.SetPtr(offset+1, *(ptrs+offset));
                        RETURNIFERROR(rc)
                    } else {
                        if (!rootkeymoved) {
                            // propagate key to root node
                            b.info.numkeys++;
                            rc = b.SetKey(0, *(keys+offset));
                            RETURNIFERROR(rc)
                            // set 0 ptr for right node
                            rc = rightnode.SetPtr(0, *(ptrs+offset));
                            RETURNIFERROR(rc)
                            rootkeymoved = 1;
                            continue;
                        }
                        rightnode.info.numkeys++;
                        rc = rightnode.SetKey(splitnodeoffset++, *(keys+offset));
                        RETURNIFERROR(rc)
                        rc = rightnode.SetPtr(splitnodeoffset, *(ptrs+offset));
                        RETURNIFERROR(rc)
                    }
                    movedcount++;
                }
                rc = b.SetPtr(0, leftptr);
                RETURNIFERROR(rc)
                rc = b.SetPtr(1, rightptr);
                RETURNIFERROR(rc)

                // Serialize back to buffer
                rc = leftnode.Serialize(buffercache, leftptr);
                RETURNIFERROR(rc)
                rc = rightnode.Serialize(buffercache, rightptr);
                RETURNIFERROR(rc)
                return b.Serialize(buffercache, node);
            }
        }
        case BTREE_INTERIOR_NODE: {
            SIZE_T maxsize = b.info.GetNumSlotsAsInterior();
            if (b.info.numkeys < maxsize) {
                b.info.numkeys++;
                for (offset=0;offset<b.info.numkeys-1;offset++) {
                    rc = b.GetKey(offset, testkey);
                    RETURNIFERROR(rc)
                    if (key < testkey) {
                        for (temp=b.info.numkeys-2; temp>=offset; temp--) {
                            rc = b.GetPtr(temp+1, tempptr);
                            RETURNIFERROR(rc)
                            rc = b.GetKey(temp, testkey);
                            RETURNIFERROR(rc)
                            rc = b.SetKey(temp+1, testkey);
                            RETURNIFERROR(rc)
                            rc = b.SetPtr(temp+2, tempptr);
                            RETURNIFERROR(rc)
                            if (temp == 0) {
                                break;
                            }
                        }
                        break;

                    } else if (key == testkey) {
                        return ERROR_CONFLICT;
                    }
                }
                rc = b.SetKey(offset, key);
                RETURNIFERROR(rc)
                rc = b.SetPtr(offset+1, ptr);
                RETURNIFERROR(rc)
                return b.Serialize(buffercache, node);
            } else {
                cerr << "Maximum num of keys in interior" << endl;
                SIZE_T splitnodeptr;
                rc = AllocateNode(splitnodeptr);
                RETURNIFERROR(rc)

                BTreeNode splitnode(BTREE_INTERIOR_NODE,
                                    superblock.info.keysize,
                                    superblock.info.valuesize,
                                    superblock.info.blocksize);
                rc = splitnode.Serialize(buffercache, splitnodeptr);
                RETURNIFERROR(rc)

                SIZE_T halfsize = (maxsize + 1) / 2;
                SIZE_T movedcount = 0;
                SIZE_T insertmoved = 0;
                SIZE_T splitnodeoffset = 0;
                SIZE_T splitfirstptr = 0;
                KEY_T *keys = new KEY_T[maxsize+1];
                SIZE_T *ptrs = new SIZE_T[maxsize+1];
                for (offset=0; offset<b.info.numkeys; offset++) {
                    rc = b.GetKey(offset, *(keys+offset+insertmoved));
                    RETURNIFERROR(rc)
                    rc = b.GetPtr(offset+1, *(ptrs+offset+insertmoved));
                    RETURNIFERROR(rc)
                    if (keys[offset+insertmoved] > key && !insertmoved) {
                        *(keys+offset+insertmoved) = key;
                        *(ptrs+offset+insertmoved) = ptr;
                        insertmoved = 1;
                        rc = b.GetKey(offset, *(keys+offset+insertmoved));
                        RETURNIFERROR(rc)
                        rc = b.GetPtr(offset+1, *(ptrs+offset+insertmoved));
                        RETURNIFERROR(rc)
                    } else if (keys[offset+insertmoved] == key) {
                        return ERROR_CONFLICT;
                    }
                }
                if (!insertmoved) {
                    *(keys+offset+insertmoved) = key;
                    *(ptrs+offset+insertmoved) = ptr;
                }
                b.info.numkeys = 0;
                KEY_T propkey;
                for (offset=0; offset<maxsize+1; offset++) {
                    if (movedcount < halfsize) {
                        b.info.numkeys++;
                        rc = b.SetKey(offset, *(keys+offset));
                        RETURNIFERROR(rc)
                        rc = b.SetPtr(offset+1, *(ptrs+offset));
                        RETURNIFERROR(rc)
                    } else {
                        if (!splitfirstptr) {
                            rc = splitnode.SetPtr(0, *(ptrs+offset));
                            RETURNIFERROR(rc)
                            propkey = *(keys+offset);
                            splitfirstptr = 1;
                            continue;
                        }
                        splitnode.info.numkeys++;
                        rc = splitnode.SetKey(splitnodeoffset++, *(keys+offset));
                        RETURNIFERROR(rc)
                        rc = splitnode.SetPtr(splitnodeoffset, *(ptrs+offset));
                        RETURNIFERROR(rc)
                    }
                    movedcount++;
                }
                // new node created, return ptr
                rc = b.Serialize(buffercache, node);
                RETURNIFERROR(rc)
                rc = splitnode.Serialize(buffercache, splitnodeptr);
                RETURNIFERROR(rc)
                // set return value
                ptr = splitnodeptr;
                key = propkey;
                // Propagate to upper level
                return ERROR_NOERROR;
            }
            break;
        }
        case BTREE_LEAF_NODE:
            return ERROR_INSANE;
        default:
            return ERROR_INSANE;
    }
    return ERROR_INSANE;
}

ERROR_T BTreeIndex::InsertKeyPtrHelper(const SIZE_T &curnode,
                                       SIZE_T &node,
                                       KEY_T &key,
                                       SIZE_T &ptr) {
    BTreeNode b;
    ERROR_T rc;
    SIZE_T offset;
    KEY_T testkey;
    SIZE_T testptr;
    SIZE_T old_ptr;

    if (curnode == node) {
        SIZE_T retptr = ptr;
        KEY_T retkey = key;
        SIZE_T retnode = node;
        rc = InsertKeyPtr(retnode, retkey, retptr);
        RETURNIFERROR(rc)
        if (retptr != ptr) {
            key = retkey;
            ptr = retptr;
            node = curnode;
        }
        return ERROR_NOERROR;
    }

    rc = b.Unserialize(buffercache, curnode);
    RETURNIFERROR(rc)

    switch (b.info.nodetype) {
        case BTREE_ROOT_NODE:
        case BTREE_INTERIOR_NODE:{
            for (offset=0;offset<b.info.numkeys;offset++) {
                rc = b.GetKey(offset, testkey);
                RETURNIFERROR(rc)
                if (key < testkey || key == testkey) {
                    break;
                }
            }
            rc = b.GetPtr(offset, testptr);
            RETURNIFERROR(rc)
            old_ptr = ptr;
            rc = InsertKeyPtrHelper(testptr, node, key, ptr);
            RETURNIFERROR(rc)
            if (old_ptr != ptr) {
                SIZE_T retptr = ptr;
                KEY_T retkey = key;
                SIZE_T retnode = curnode;
                rc = InsertKeyPtr(retnode, retkey, retptr);
                RETURNIFERROR(rc)
                if (retptr != ptr) {
                    key = retkey;
                    ptr = retptr;
                    node = curnode;
                }
                return ERROR_NOERROR;
            }
            break;
        }
        case BTREE_LEAF_NODE:
            return ERROR_INSANE;
    }
    return ERROR_NOERROR;
}

ERROR_T BTreeIndex::InsertHelper(SIZE_T &node, KEY_T &key, const VALUE_T &value)
{
    BTreeNode b;
    ERROR_T rc;
    SIZE_T offset;
    SIZE_T temp;
    KEY_T testkey;
    SIZE_T ptr;

    rc = b.Unserialize(buffercache, node);

    if (rc != ERROR_NOERROR) {
        return rc;
    }

    switch (b.info.nodetype) {
        case BTREE_ROOT_NODE: {
            // No keys so far, need to allocate node or go into first leaf node
            if (b.info.numkeys == 0) {
                rc = b.GetPtr(0, ptr);
                RETURNIFERROR(rc)
                // need to alloc node
                if (ptr != node+1)  {
                    rc = AllocateNode(ptr);
                    RETURNIFERROR(rc)
                    BTreeNode b_leaf(BTREE_LEAF_NODE,
                                     superblock.info.keysize,
                                     superblock.info.valuesize,
                                     superblock.info.blocksize);
                    b_leaf.Serialize(buffercache, ptr);
                    rc = b.SetPtr(0, ptr);
                    RETURNIFERROR(rc)

                    rc = b.Serialize(buffercache, node);
                    RETURNIFERROR(rc)
                }
                SIZE_T old_ptr = ptr;
                rc = InsertHelper(ptr, key, value);
                RETURNIFERROR(rc)
                if (old_ptr != ptr) {
                    // leaf node split, insert new key
                    b.info.numkeys++;
                    rc = b.SetKey(0, key);
                    RETURNIFERROR(rc)
                    rc = b.SetPtr(1, ptr);
                    RETURNIFERROR(rc)
                    return b.Serialize(buffercache, node);
                }
                break;
            }
        }
        case BTREE_INTERIOR_NODE: {
            SIZE_T old_ptr;
            for (offset=0;offset<b.info.numkeys;offset++) {
                rc = b.GetKey(offset, testkey);
                RETURNIFERROR(rc)
                if (key<testkey || key==testkey) {
                    rc = b.GetPtr(offset, ptr);
                    RETURNIFERROR(rc)
                    old_ptr = ptr;
                    rc = InsertHelper(ptr, key, value);
                    RETURNIFERROR(rc)
                    if (old_ptr != ptr) {
                        return InsertKeyPtrHelper(superblock.info.rootnode,
                                            node, key, ptr);
                    } else {
                        return ERROR_NOERROR;
                    }
                }
            }
            // Did't find any greater keys, go to last ptr
            rc = b.GetPtr(b.info.numkeys, ptr);
            RETURNIFERROR(rc)
            old_ptr = ptr;
            rc = InsertHelper(ptr, key, value);
            RETURNIFERROR(rc)
            if (old_ptr != ptr) {
                // leaf node split, insert new key and ptr
                return InsertKeyPtrHelper(superblock.info.rootnode,
                                    node, key, ptr);
            }
            return ERROR_NOERROR;
        }
        case BTREE_LEAF_NODE: {
            if (b.info.numkeys < b.info.GetNumSlotsAsLeaf()) {
                b.info.numkeys++;
                for (offset=0;offset<b.info.numkeys-1;offset++) {
                    rc = b.GetKey(offset, testkey);
                    RETURNIFERROR(rc)
                    if (key < testkey) {
                        for (temp=b.info.numkeys-2; temp>=offset; temp--) {
                            KeyValuePair tempkeyval;
                            rc = b.GetKeyVal(temp, tempkeyval);
                            RETURNIFERROR(rc)
                            rc = b.SetKeyVal(temp+1, tempkeyval);
                            RETURNIFERROR(rc)
                            // Avoid overflow
                            if (temp == 0) {
                                break;
                            }
                        }
                        break;
                    }
                    if (key == testkey) {
                        return ERROR_CONFLICT;
                    }
                }
//                KeyValuePair keyvalpair(key, value);
//                rc = b.SetKeyVal(offset, keyvalpair);
                rc = b.SetKey(offset, key);
                RETURNIFERROR(rc)
                rc = b.SetVal(offset, value);
                if (rc) {
                    return rc;
                } else {
                    rc = b.Serialize(buffercache, node);
                    return rc;
                }
            } else { // split
                SIZE_T maxsize= b.info.GetNumSlotsAsLeaf();
                cerr << "Maximum slots (";
                cerr << maxsize;
                cerr << ")" << endl;
                cerr << "Need to split" << endl;

                rc = AllocateNode(ptr);
                RETURNIFERROR(rc)
                BTreeNode splitnode(BTREE_LEAF_NODE,
                    superblock.info.keysize,
                    superblock.info.valuesize,
                    superblock.info.blocksize);;
                rc = splitnode.Serialize(buffercache, ptr);
                RETURNIFERROR(rc)

                // Moving objects
                SIZE_T halfsize = (maxsize + 1) / 2;
                SIZE_T movedcount = 0;
                SIZE_T insertmoved = 0;
                SIZE_T splitnodeoffset = 0;
                KeyValuePair *tempkeyval;
                tempkeyval = new KeyValuePair [b.info.numkeys+1];
                for (offset=0; offset<b.info.numkeys; offset++) {
                    rc = b.GetKeyVal(offset, *(tempkeyval+offset+insertmoved));
                    RETURNIFERROR(rc)
                    if (tempkeyval[offset+insertmoved].key > key && !insertmoved) {
                        *(tempkeyval+offset+insertmoved) = KeyValuePair(key, value);
                        insertmoved = 1;
                        rc = b.GetKeyVal(offset, *(tempkeyval+offset+insertmoved));
                        RETURNIFERROR(rc)
                    } else if (tempkeyval[offset+insertmoved].key == key) {
                        return ERROR_CONFLICT;
                    }

                }
                if (!insertmoved) {
                    *(tempkeyval+offset+insertmoved) = KeyValuePair(key, value);
                }
                b.info.numkeys = 0;
                for (offset=0; offset<maxsize+1; offset++) {
                    if (movedcount < halfsize) {
                        b.info.numkeys++;
                        rc = b.SetKeyVal(offset, *(tempkeyval+offset));
                        RETURNIFERROR(rc)
                    } else {
                        splitnode.info.numkeys++;
                        rc = splitnode.SetKeyVal(splitnodeoffset++, *(tempkeyval+offset));
                        RETURNIFERROR(rc)
                    }
                    movedcount++;
                }
                // new node created, return ptr
                rc = b.Serialize(buffercache, node);
                RETURNIFERROR(rc)
                rc = splitnode.Serialize(buffercache, ptr);
                RETURNIFERROR(rc)
                // set return value
                node = ptr;
                rc = splitnode.GetKey(0, key);
                RETURNIFERROR(rc)
                return ERROR_NOERROR;
            }
            break;
        }
        default:
            return ERROR_INSANE;
    }
    return ERROR_NOERROR;
}

ERROR_T BTreeIndex::InsertInternal(const SIZE_T &node, const BTreeOp op, const KEY_T &key, const VALUE_T &value) {
    if (op != BTREE_OP_INSERT) {
        return ERROR_INSANE;
    }
    ERROR_T rc;
    KEY_T retkey = key;
    SIZE_T retnode = node;
    rc = InsertHelper(retnode, retkey, value);
    return rc;
}

static ERROR_T PrintNode(ostream &os, SIZE_T nodenum, BTreeNode &b, BTreeDisplayType dt)
{
  KEY_T key;
  VALUE_T value;
  SIZE_T ptr;
  SIZE_T offset;
  ERROR_T rc;
  unsigned i;

  if (dt==BTREE_DEPTH_DOT) { 
    os << nodenum << " [ label=\""<<nodenum<<": ";
  } else if (dt==BTREE_DEPTH) {
    os << nodenum << ": ";
  } else {
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (dt==BTREE_SORTED_KEYVAL) {
    } else {
      if (dt==BTREE_DEPTH_DOT) { 
      } else { 
	os << "Interior: ";
      }
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	os << "*" << ptr << " ";
	// Last pointer
	if (offset==b.info.numkeys) break;
	rc=b.GetKey(offset,key);
	if (rc) {  return rc; }
	for (i=0;i<b.info.keysize;i++) { 
	  os << key.data[i];
	}
	os << " ";
      }
    }
    break;
  case BTREE_LEAF_NODE:
    if (dt==BTREE_DEPTH_DOT || dt==BTREE_SORTED_KEYVAL) { 
    } else {
      os << "Leaf: ";
    }
    for (offset=0;offset<b.info.numkeys;offset++) { 
      if (offset==0) { 
	// special case for first pointer
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (dt!=BTREE_SORTED_KEYVAL) { 
	  os << "*" << ptr << " ";
	}
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << "(";
      }
      rc=b.GetKey(offset,key);
      if (rc) {  return rc; }
      for (i=0;i<b.info.keysize;i++) { 
	os << key.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ",";
      } else {
	os << " ";
      }
      rc=b.GetVal(offset,value);
      if (rc) {  return rc; }
      for (i=0;i<b.info.valuesize;i++) { 
	os << value.data[i];
      }
      if (dt==BTREE_SORTED_KEYVAL) { 
	os << ")\n";
      } else {
	os << " ";
      }
    }
    break;
  default:
    if (dt==BTREE_DEPTH_DOT) { 
      os << "Unknown("<<b.info.nodetype<<")";
    } else {
      os << "Unsupported Node Type " << b.info.nodetype ;
    }
  }
  if (dt==BTREE_DEPTH_DOT) { 
    os << "\" ]";
  }
  return ERROR_NOERROR;
}
  
ERROR_T BTreeIndex::Lookup(const KEY_T &key, VALUE_T &value)
{
  return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_LOOKUP, key, value);
}

ERROR_T BTreeIndex::Insert(const KEY_T &key, const VALUE_T &value)
{
  // WRITE ME
  return InsertInternal(superblock.info.rootnode, BTREE_OP_INSERT, key, value);
}
  
ERROR_T BTreeIndex::Update(const KEY_T &key, const VALUE_T &value)
{
    VALUE_T v(value);
    return LookupOrUpdateInternal(superblock.info.rootnode, BTREE_OP_UPDATE, key, v);
}

  
ERROR_T BTreeIndex::Delete(const KEY_T &key)
{
  // This is optional extra credit 
  //
  // 
  return ERROR_UNIMPL;
}

  
//
//
// DEPTH first traversal
// DOT is Depth + DOT format
//

ERROR_T BTreeIndex::DisplayInternal(const SIZE_T &node,
				    ostream &o,
				    BTreeDisplayType display_type) const
{
  KEY_T testkey;
  SIZE_T ptr;
  BTreeNode b;
  ERROR_T rc;
  SIZE_T offset;

  rc= b.Unserialize(buffercache,node);

  if (rc!=ERROR_NOERROR) { 
    return rc;
  }

  rc = PrintNode(o,node,b,display_type);
  
  if (rc) { return rc; }

  if (display_type==BTREE_DEPTH_DOT) { 
    o << ";";
  }

  if (display_type!=BTREE_SORTED_KEYVAL) {
    o << endl;
  }

  switch (b.info.nodetype) { 
  case BTREE_ROOT_NODE:
  case BTREE_INTERIOR_NODE:
    if (b.info.numkeys>0 || b.info.numkeys==0) {
      for (offset=0;offset<=b.info.numkeys;offset++) { 
	rc=b.GetPtr(offset,ptr);
	if (rc) { return rc; }
	if (display_type==BTREE_DEPTH_DOT) { 
	  o << node << " -> "<<ptr<<";\n";
	}
	rc=DisplayInternal(ptr,o,display_type);
	if (rc) { return rc; }
      }
    }
    return ERROR_NOERROR;
    break;
  case BTREE_LEAF_NODE:
    return ERROR_NOERROR;
    break;
  default:
    if (display_type==BTREE_DEPTH_DOT) { 
    } else {
      o << "Unsupported Node Type " << b.info.nodetype ;
    }
    return ERROR_INSANE;
  }

  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::Display(ostream &o, BTreeDisplayType display_type) const
{
  ERROR_T rc;
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "digraph tree { \n";
  }
  rc=DisplayInternal(superblock.info.rootnode,o,display_type);
  if (display_type==BTREE_DEPTH_DOT) { 
    o << "}\n";
  }
  return ERROR_NOERROR;
}


ERROR_T BTreeIndex::SanityCheck() const
{
  // WRITE ME
  return ERROR_UNIMPL;
}
  


ostream & BTreeIndex::Print(ostream &os) const
{
  // WRITE ME
  return os;
}




