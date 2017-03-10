#include <stdlib.h>
#include "btree.h"

void usage() 
{
  cerr << "usage: btree_display filestem cachesize dot|normal\n";
}


int main(int argc, char **argv)
{
  char *filestem;
  bool dot;
  SIZE_T cachesize;
  SIZE_T superblocknum;

  if (argc!=4) { 
    usage();
    return -1;
  }

  filestem=argv[1];
  cachesize=atoi(argv[2]);
  dot=argv[3][0]=='d' || argv[3][0]=='D';

  DiskSystem disk(filestem);
  BufferCache cache(&disk,cachesize);
  BTreeIndex btree(0,0,&cache);
  
  ERROR_T rc;


  if ((rc=cache.Attach())!=ERROR_NOERROR) { 
    cerr << "Can't attach buffer cache due to error"<<rc<<endl;
    return -1;
  }

  if ((rc=btree.Attach(0))!=ERROR_NOERROR) { 
    cerr << "Can't attach to index  due to error "<<rc<<endl;
    return -1;
  } else {
    cerr << "Index attached!"<<endl;
    VALUE_T val;
    if ((rc=btree.Display(cout, dot ? BTREE_DEPTH_DOT : BTREE_DEPTH))!=ERROR_NOERROR) { 
      cerr <<"Display failed: error "<<rc<<endl;
    } else {
      cerr <<"Display succeeded\n";
    }
    if ((rc=btree.Detach(superblocknum))!=ERROR_NOERROR) { 
      cerr <<"Can't detach from index due to error "<<rc<<endl;
      return -1;
    }
    if ((rc=cache.Detach())!=ERROR_NOERROR) { 
      cerr <<"Can't detach from cache due to error "<<rc<<endl;
      return -1;
    }
    cerr << "Performance statistics:\n";
    
    cerr << "numallocs       = "<<cache.GetNumAllocs()<<endl;
    cerr << "numdeallocs     = "<<cache.GetNumDeallocs()<<endl;
    cerr << "numreads        = "<<cache.GetNumReads()<<endl;
    cerr << "numdiskreads    = "<<cache.GetNumDiskReads()<<endl;
    cerr << "numwrites       = "<<cache.GetNumWrites()<<endl;
    cerr << "numdiskwrites   = "<<cache.GetNumDiskWrites()<<endl;
    cerr << endl;
    
    cerr << "total time      = "<<cache.GetCurrentTime()<<endl;

    return 0;
  }
}
  

  
