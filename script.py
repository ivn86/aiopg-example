import asyncio

import sqlalchemy as sa

from aiopg.sa import create_engine
from sqlalchemy.sql.ddl import CreateTable

metadata = sa.MetaData()

customers = sa.Table('customers', metadata,
                     sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
                     sa.Column('name', sa.String(255), index=True))

products = sa.Table('products', metadata,
                    sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
                    sa.Column('device', sa.String(255), index=True),
                    sa.Column('manufacturer', sa.String(255), index=True),
                    sa.Column('price', sa.Integer))

orders = sa.Table('orders', metadata,
                  sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
                  sa.Column('customer_id', None, sa.ForeignKey('customers.id')),
                  sa.Column('product_id', None, sa.ForeignKey('products.id')),
                  sa.Column('product_count', sa.Integer, default=1))


async def create_tables(conn):
    await conn.execute('DROP TABLE IF EXISTS orders')
    await conn.execute('DROP TABLE IF EXISTS customers')
    await conn.execute('DROP TABLE IF EXISTS products')

    await conn.execute(CreateTable(customers))
    await conn.execute(CreateTable(products))
    await conn.execute(CreateTable(orders))


shop_products = [
    {'device': 'TV', 'manufacturer': 'Sony', 'price': 100},
    {'device': 'Laptop', 'manufacturer': 'Sony', 'price': 1000},

    {'device': 'Laptop', 'manufacturer': 'Samsung', 'price': 500},
    {'device': 'Smartphone', 'manufacturer': 'Samsung', 'price': 100},
    {'device': 'TV', 'manufacturer': 'Samsung', 'price': 160},
    {'device': 'Monitor', 'manufacturer': 'Samsung', 'price': 300},

    {'device': 'Laptop', 'manufacturer': 'DELL', 'price': 550},
    {'device': 'Monitor', 'manufacturer': 'DELL', 'price': 400},
]


async def init_data(conn):
    async with conn.begin():
        for name in ['John', 'Alice']:
            await conn.execute(customers.insert().values(
                name=name
            ))

        for product_dict in shop_products:
            await conn.execute(products.insert().values(
                device=product_dict.get('device'),
                manufacturer=product_dict.get('manufacturer'),
                price=product_dict.get('price')
            ))


async def action_data(conn):
    async with conn.begin():
        await conn.execute(orders.insert().values(
            customer_id=1,
            product_id=1
        ))
        await conn.execute(orders.insert().values(
            customer_id=2,
            product_id=2,
            product_count=2
        ))
        await conn.execute(orders.insert().values(
            customer_id=2,
            product_id=4,
            product_count=4
        ))

        await conn.execute(sa.update(customers).values({"name": "Bob"}).where(customers.c.name == "Alice"))


async def report(conn):
    query = sa.select([
        customers.c.name,
        products.c.device,
        products.c.manufacturer,
        (products.c.price * orders.c.product_count).label('sum')
    ]).select_from(
        sa.join(customers, orders, customers.c.id == orders.c.customer_id).\
            join(products, products.c.id == orders.c.product_id)
    )
    print("========== REPORT ==========")
    async for row in conn.execute(query):
        print(f"{row.name}:\t {row.manufacturer}, {row.device} == {row.sum}")


async def go():
    engine = await create_engine(user='aiopg',
                                 database='aiopg',
                                 host='127.0.0.1',
                                 password='aiopg')
    async with engine:
        async with engine.acquire() as conn:
            await create_tables(conn)
            await init_data(conn)
            await action_data(conn)
            await report(conn)


loop = asyncio.get_event_loop()
loop.run_until_complete(go())
