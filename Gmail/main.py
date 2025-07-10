import ezgmail
import pandas as pd
from bs4 import BeautifulSoup as bs
import os
from to_s3_txt import s3
from to_s3_attachments import attach
from get_link import link
from database import conn


df=[]
mails=ezgmail.search('in:inbox category:primary is:unread newer_than:3d')
folder_attachments=r'C:\Users\Krishna\Desktop\Gmail\attachments'
folder_txt=r'C:\Users\Krishna\Desktop\Gmail\mail_txt'
for mail in mails:
    for i in mail.messages:
        sender=i.sender
        sender_1=sender.replace('"','').replace('<','').replace('>','').replace(':','%3A')
        receiver=i.recipient
        subject=i.subject
        body = bs(i.body, 'html.parser').get_text() if i.body else '[No body content found]'
        time=i.timestamp
        id=i.id
        text=sender+'\n'+receiver+'\n'+subject+'\n'+body+'\n'
        file_path=os.path.join(folder_txt,f"{i.id}.txt")
        with open(file_path,'w',encoding='utf-8') as f:
            f.write(text)
        links=''
        if i.attachments:
            i.downloadAllAttachments(folder_attachments)
            for j in i.attachments:
                attach(sender_1,time,id,folder_attachments,j)
                links=link(sender_1,time,id,folder_attachments,j)


        df.append({
            'sender':sender,
            'receiver':receiver,
            'subject':subject,
            'body':body,
            'timestamp':time,
            'attachment_link':links
        })
        s3(sender_1,time,id,folder_txt)
    mail.markAsRead()
dataset=pd.DataFrame(df)
dataset.to_excel('dataset.xlsx', index=False)
engine=conn()
dataset.to_sql('gmail_data',engine,if_exists='append',index=False)
