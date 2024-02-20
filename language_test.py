import datetime
import pytz
from datetime import timezone

now = datetime.datetime.now()#timezone.utc)
print('now = datetime.datetime.now()')
print(now)
print('type: %s\n' %type(now))
print(pytz.all_timezones_set)

now_with_timezone = pytz.timezone('Asia/Taipei').localize(now)

print(now_with_timezone)
print('type: %s\n'%type(now_with_timezone))


iso_now = now.isoformat()
print('now.isoformat()')
print(iso_now)
print('type: %s\n' %type(iso_now))

if iso_now is not None and isinstance(iso_now, str):
    print('iso_now is string\nconvert to datetime')
    convert_time = datetime.datetime.fromisoformat(iso_now)
    print('datetime.datetime.fromisoformat(iso_now)')
    print(convert_time)
    print(type(convert_time))
    
dt_str = '2024-02-07 14:46:00'


a = [''] * 3#['a','b','c']
b = ['1','2','3']
for c in zip(a,b):
    print(c)
    print(c[0])
    print(c[1])
#     print(c[0])
#     print(c[1])