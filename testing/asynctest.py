import asyncio
import time


async def task(number):
    print("Sub Task %d" % number)


async def task1(runs):
    print("Task 1")
    packaged_subtask1 = asyncio.create_task(subtask(1))
    print("Tasks :" + str(asyncio.all_tasks()))
    for i in range(runs):
        # await [coro] adds task to next in stack and runs next in stack until [coro] is finished
        await subtask(0)
        # await [task] if finished does not run the stack, returning whatever it is supposed to output
        await packaged_subtask1
        await subtask(2)


async def subtask(number):
    print("Sub Task %d" % number)


async def task2():
    print("Task 2")
    await wait(3)


async def task3():
    print("Task 3")
    return "Returned"


async def task_looping():
    a = asyncio.create_task(taska())
    b = asyncio.create_task(taskb())
    c = asyncio.create_task(taskc())

    await a, b, c


async def taska():
    for i in range(10):
        packed = asyncio.create_task(subtask(i))
        await packed
        print("a looped")


async def taskb():
    for i in range(10):
        packed = asyncio.create_task(taskblock())
        await packed
        print("b looped")


async def taskc():
    for i in range(10):
        packed = asyncio.create_task(subtask(i))
        await packed
        print("c looped")


async def taskblock():
    print("io started")
    for i in range(3):
        print("blocked")
        await asyncio.sleep(1)
        print("check again")


async def wait(t):
    time.sleep(t)
    await asyncio.sleep(1)
    # Subtask 7 will be printed after Full Execute
    packaged_subtask1 = asyncio.create_task(subtask(7))
    await subtask(9)
    print("Full Execute")


async def asyncwait(t):
    await asyncio.sleep(t)
    print("Force Return")


async def main():
    # add tasks to bottom of the stack
    packaged_task1 = asyncio.create_task(task1(5))
    packaged_task2 = asyncio.create_task(task2())
    packaged_task3 = asyncio.create_task(task3())


    print(f"started at {time.strftime('%X')}")

    # await.sleep() runs the whole stack
    await asyncwait(2)
    # await [task] runs next task in stack, until whole stack is done once or until [task] fully executes while others in stack do not required to finish
    await packaged_task3
    print("Between Tasks")
    # await [task] if finished does not run the stack, returning whatever it is supposed to output
    answer = await packaged_task3
    print(answer)
    await task(4)
    packaged_task5 = asyncio.create_task(task(5))
    await packaged_task5

    packaged_wait = asyncio.create_task(wait(5))
    packaged_wait2 = asyncio.create_task(wait(5))
    await asyncwait(1)

    print(f"finished at {time.strftime('%X')}")
    # if there are still tasks in stack after main it will run them to completion in order


if __name__ == "__main__":
    # asyncio.run(main())
    asyncio.run(task_looping())
    print("done")
