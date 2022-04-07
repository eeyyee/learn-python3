from models import Comment, User, Blog
import orm
import asyncio

async def test():
    loop = asyncio.get_event_loop()
    await orm.createPool(loop=loop, user='zhanghao', password='zhanghao', db='awesome')

    u = User(name='Test', email='test@example.com', passwd='1234567890', image='about:blank')

    await u.save()

loop = asyncio.get_event_loop()
loop.run_until_complete(test())