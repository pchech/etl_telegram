#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
from database import CursorFromPool,Database
from datetime import datetime
from telethon import TelegramClient, sync,events,utils
import asyncio
import os.path
import traceback
from time import sleep
import nltk
import re


class TelegramAPI:
    api_id = None
    api_hash = None
    app_name = None
    links_from_db = None
    client = None
    file = "/u01/ETL/LOCK/F_STOP.txt"
    order = None
    #cur = None
    #cfp = None
    dialog_dict = {}
    pattern = '(http(s)?://[^\"\s>]+)'

    @classmethod
    def initialize(cls):
        print("Init")
        with CursorFromPool() as cur:
            #cfp = CursorFromPool()
            #cur = cls.cfp.__enter__()
            print("Get cursor")
            cur.execute("SELECT link FROM sa.d_telegram_links  ds WHERE status = 'ACTIVE';")
            cls.links_from_db = [link[0] for link in cur.fetchall()]
            print(cls.links_from_db)
            cur.execute("SELECT * FROM sa.d_order WHERE source_id = -8")
            cls.order = cur.fetchone()[0]

            print(cls.order)
            cur.execute("SELECT value FROM sa.d_params WHERE api = 'Telegram API' and name = 'id';")
            cls.api_id = cur.fetchone()[0]

            print(cls.api_id)

            cur.execute("SELECT value FROM sa.d_params WHERE api = 'Telegram API' and name = 'hash';")
            cls.api_hash = cur.fetchone()[0]

            print(cls.api_hash)
            cur.execute("SELECT value FROM sa.d_params WHERE api = 'Telegram API' and name = 'name';")
            cls.app_name = cur.fetchone()[0]


            print(cls.app_name)
            cls.client = TelegramClient(cls.app_name, cls.api_id, cls.api_hash)
            
            

            @cls.client.on(events.NewMessage(chats=cls.links_from_db))
            async def normal_handler_VA(event):
                await cls.on_message(event)

    @classmethod
    async def on_message(cls,event):
        print(event.message.to_dict())
        msg = event.message.to_dict()
        if msg["message"]!='':
            with CursorFromPool() as cur:
                insertQuery = """INSERT INTO sa.t_telegram_news(pubtype_id,post_id,author_id,text,creationtime, 
                eventlink,order_id,reply_owner_id,title) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

                if msg["fwd_from"] is None:
                    pubtype_id = 2
                else:
                    pubtype_id = 1

                post_id = msg["id"]
                author_id=""
                eventlink = ""
                if msg["from_id"] is not None:
                    if msg["from_id"]["_"] == 'PeerUser':
                        sender = await event.get_sender()  # получаем имя юзера
                        author_id = utils.get_display_name(sender)  # Имя Юзера
                    elif msg["from_id"]["_"] == 'PeerChat':
                        #chat_from = await event.get_chat()  # получаем имя группы
                        #author_id = utils.get_display_name(chat_from)  # получаем имя группы
                        author_id= cls.dialog_dict[msg["peer_id"]["channel_id"]]["name"]
                        eventlink = cls.dialog_dict[msg["peer_id"]["channel_id"]]["username"]
                else:
                    #chat_from = await event.get_chat()  # получаем имя группы
                    #author_id = utils.get_display_name(chat_from)  # получаем имя группы
                    author_id= cls.dialog_dict[msg["peer_id"]["channel_id"]]["name"]
                    eventlink = cls.dialog_dict[msg["peer_id"]["channel_id"]]["username"]

                text = msg["message"]

                text_tokens = nltk.sent_tokenize(re.sub(' ч\.',' часть',re.sub(' ст\.',' статья',text)))
                title = re.sub(cls.pattern,'',text_tokens[0]).replace('\n','').rstrip('.')
                text = ' '.join(text_tokens[1:])
                creationtime = msg["date"]

                eventlink = eventlink + "/" + str(msg["id"])
                order_id = cls.order
                reply_owner_id = None
                if msg["reply_to"] is not None:
                    reply_owner_id = msg["reply_to"]["reply_to_msg_id"]
                #fwd_owner_id = None
                #if msg["fwd_from"] is not None:
                #    fwd_owner_id = msg["fwd_from"]["from_id"]
                if title != '':
                    insertQueryMogrify=cur.mogrify(insertQuery,(pubtype_id,post_id,author_id,text,creationtime,eventlink,order_id,reply_owner_id,title))
                    print(insertQueryMogrify)
                    cur.execute(insertQueryMogrify)
    @classmethod
    async def get_dialogs(cls):
        async for dialog in cls.client.iter_dialogs():
            d={}
            #print(dialog.entity)
            d["name"] = dialog.name
            d["username"] = dialog.entity.username
            cls.dialog_dict[dialog.entity.id] = d
        print(cls.dialog_dict)

    @classmethod
    async def infinity_loop(cls):
        print("start loop")
        await cls.client.start(password='')
        await cls.get_dialogs()
        while True:
            if os.path.isfile(cls.file):
                print("find control")
                break
            await asyncio.sleep(1)
        print("exit loop")
        await cls.client.disconnect()
        #cls.cfp.__exit__()
        print("disconnect")

    @classmethod
    def main(cls):
        print("start main")
        aio_loop = asyncio.get_event_loop()
        try:
            aio_loop.run_until_complete(TelegramAPI.infinity_loop())
            print("stop main")
        finally:
            if not aio_loop.is_closed():
                aio_loop.close()

try:
    Database.initialise()
    TelegramAPI.initialize()
    TelegramAPI.main()
except Exception as e:
    print("Error in telegram app.py")
    traceback.print_exc()
