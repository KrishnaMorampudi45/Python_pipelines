import json
with open(r'C:\Users\Krishna\Desktop\New folder\data\project.txt','r') as data:
    json_data=data.read()
data=json.loads(json_data)
    
with open('data2.json','w') as f:
    json.dump(data,f,indent=4,sort_keys=False)