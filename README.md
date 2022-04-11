# burrowCheck

## 1. 개요
### burrow
kafka consumer의 lag 관제를 위해, 보통 오픈소스인 burrow를 사용합니다.
이 때, burrow는 그 시점의 lag만 관제할 뿐, 따로 (lag관련)로그를 남기거나 파일로 떨어뜨리지 않습니다.

### burrowCheck
그래서 burrowCheck는 burrow를 쉽게 사용하기 위한 tool입니다.

1) burrowCheck는 burrow API를 호출하고
2) API 호출 결과인 json을 추출하여 로그로 기록.
3) 에러 발생 시, Telegram으로 메세지 전송.
위 3가지 단계를 수행합니다.

elk stack을 이용해, 좀더 체계적으로 burrow를 사용할 수 있습니다만, 
burrowCheck는 elk에 비해 가볍게 사용하기 위해 만들게 되었습니다.

## 2. 동작 방식

