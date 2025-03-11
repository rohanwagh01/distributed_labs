package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck     kvtest.IKVClerk
	key    string
	client string
}

const OPEN = "OPEN"

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, key: l}
	// add a client identifier to the lock
	lk.client = kvtest.RandValue(8)
	return lk
}

func (lk *Lock) Acquire() {
	//put in infinite loop and only return when lock is acquired
	for {
		val, ver, err := lk.ck.Get(lk.key)
		//before acquiring, see if it already updated (handle a prior errmaybe)
		if val == lk.client {
			return
		}
		if val == OPEN || err == rpc.ErrNoKey { //if lock is open or the kv has not been made yet
			//try putting client as value on lock key
			putErr := lk.ck.Put(lk.key, lk.client, ver)
			//fmt.Println(putErr)
			if putErr == rpc.OK { //if the client is succesfully added, then return
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	val, ver, err := lk.ck.Get(lk.key)
	if err == rpc.OK { //recieved successfully
		if val == lk.client { //this client holds the lock currently
			lk.ck.Put(lk.key, OPEN, ver)
			//fmt.Println(putErr)
		}
	}
}
