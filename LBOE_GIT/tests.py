#!/usr/bin/env python
"""
Simple producer-consumer model realized with Queues
"""
import threading
from Queue import Queue

class FactoryBase(object):
    def __init__(self, th_count = 2, q_size = 10):
        self._th_cnt = th_count
        self._q_size = q_size
        
        # Change this to actuals
        self._sh_resource = [[1,2,3],[2,3,1],[5,4,5],[7,7,8],[4,2,9]]
        
        if self.__invokeWorkers__():
            raise ValueError, "Could not complete thread creation" 
        
    def __pick_and_match__(self, queue):
        while(True):
            if not queue.empty():
                print "Match ", queue.get(), "and ", self._sh_resource
            else:
                print "Nothing yet"
        
    # Call this to start worker threads with one queue each 
    def __invokeWorkers__(self):
        for thIndex in range(self._th_cnt):
            try:
                # Create thread, attach target and queue
                activeThread = threading.Thread(name = "Thread_%d"%(thIndex),
                                                target = self.__pick_and_match__,
                                                args = (Queue(),))
                
                # Start and make main thread wait
                activeThread.start()
                activeThread.join()
            except Exception as ex:
                print ex
                return 1            
        return 0

if __name__ == "__main__":
    task_object = FactoryBase()    
