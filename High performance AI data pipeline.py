#---------------------------------------AI DATA PIPELINE ------------------------------------------------

import aiohttp 
from multiprocessing import Pool , cpu_count , Queue , Process 
import asyncio 
import threading

fetching_queue = asyncio.Queue(maxsize=100) #all data from fetching willbe stores here

#-------------------------------Fetcher-----------------------------------------

async def fetch(session, url):

    async with session.get(url) as response :
        data = await response.json()
        
        for post in data :
            body = post.get("body", "") #some end points dont have body so handles safely
            data_unit = (post["id"], body)
            await fetching_queue.put(data_unit)
        
#---------------------------------------------------------------------------

async def main():
    
    urls = ["https://jsonplaceholder.typicode.com/posts",
            "https://jsonplaceholder.typicode.com/comments",
            "https://jsonplaceholder.typicode.com/users"]
    
    async with aiohttp.ClientSession() as session : 
        tasks = [fetch(session , url) for url in urls]
        await asyncio.gather(*tasks,) 
        
    await fetching_queue.put(None)
    
#-----------------------------BRIDGE---------------------------------------------
def create_mp_queue():
    return Queue()

async def bridge(fetching_queue , mp_queue , N):
    batch = []
    BATCH_SIZE = 100
    loop = asyncio.get_running_loop()

    while True :
        item = await fetching_queue.get()

        if item is None : 
            if batch :
                await loop.run_in_executor(None , mp_queue.put , batch)  

            for _ in range(N):
                await loop.run_in_executor(None , mp_queue.put , None) 
            break
    
        batch.append(item)

        if len(batch) >= BATCH_SIZE : 
            await loop.run_in_executor(None , mp_queue.put, batch)
            batch = [] #new list , avoids mutation issues 

#-----------------------------WORKER--------------------------------------------------

def worker_loop(mp_queue , result_queue):
    while True :
        Batch = mp_queue.get()
    
        if Batch is None : 
            result_queue.put(None)
            break 

        batch_freq = {}

        for item in Batch :
            post_id , text = item  #item is a tuple 
            words = text.lower().split()

        
            for word in words :
                batch_freq[word] = batch_freq.get(word , 0) + 1 

        result_queue.put(batch_freq)   # it iterates through the list, extracts text from each tuple, and combines everything into one dictionary per batch

#----------------------------------Runner-------------------------------------------------------

async def runner(mp_queue , N):
    await asyncio.gather(main() , bridge(fetching_queue , mp_queue, N))


#---------------------------------Aggregator----------------------------------------------------

def aggregator(result_queue , N , final):
    finished_workers = 0 

    while finished_workers < N :
        item = result_queue.get()

        if item is None :
            finished_workers += 1 

        else : 
            for word , count in item.items():
                final[word] = final.get(word , 0) + count 
    
#_____________________________________MAIN_____________________________________________________

if __name__ == "__main__":
    mp_queue = create_mp_queue()
    result_queue = create_mp_queue()

    cores = cpu_count()
    N = cores

    #start workers
    workers = []                                         
    for _ in range(N):
        p = Process(target = worker_loop , args = (mp_queue , result_queue))
        p.start()
        workers.append(p)

    #start aggregator thread 

    final  = {}
    agg_thread = threading.Thread(target= aggregator , args=(result_queue, N , final)) #threads dont return values we need a shared container 
    agg_thread.start()

    #running them parallely
    asyncio.run(runner(mp_queue , N))

#------final----------

    for p in workers:
        p.join()

    agg_thread.join() 

    for word , count in final.items() :
        print(f"{word} : {count}")


