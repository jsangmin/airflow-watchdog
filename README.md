# Airflow Watchdog

**Airflow Watchdog**은 Apache Airflow 환경의 운영 건전성을 모니터링하고, 실행 이력 및 향후 스케줄을 보고하기 위해 설계된 시스템 DAG 컬렉션입니다.

이 프로젝트는 Airflow 운영자(Operator)가 클러스터 상태를 파악하고 잠재적인 문제를 조기에 발견할 수 있도록 돕습니다.

## 📋 DAG 목록

이 저장소에는 다음과 같은 모니터링용 DAG들이 포함되어 있습니다.

### 1. Airflow Ops Monitor (`airflow_ops_monitor`)
Airflow의 전반적인 건강 상태를 주기적으로 체크합니다.
- **Import Errors**: 문법 오류 등으로 인해 로드되지 않는 DAG을 감지합니다 (DB 조회 방식 최적화 적용).
- **Stuck Tasks**: 30분 이상 Queued 상태이거나 1시간 이상 Running 상태인 태스크를 감지합니다.
- **Failed Runs**: 최근 24시간 내 실패한 DAG 실행 이력을 리포팅합니다.
- **Schedule**: `@hourly`

### 2. Daily History Collector (`daily_dag_history_collector`)
매일 하루 동안 실행된 모든 DAG와 Task의 상세 이력을 수집합니다.
- **수집 항목**: 실행 상태, 시작/종료 시간, 소요 시간, Retry 횟수, 로그 URL, 실행 워커(Hostname) 등.
- **활용**: 일일 운영 리포트 작성 또는 장기 데이터 분석용 로그 저장.
- **Schedule**: 매일 23:50

### 3. Upcoming Schedule Checker (`upcoming_schedule_checker`)
활성화된(Active) DAG들의 다음 실행 예정 시간을 한눈에 파악합니다.
- **기능**: 현재 Pause 되지 않은 DAG들의 Next Execution Date를 시간순으로 정렬하여 보여줍니다.
- **Schedule**: 매일 07:00

## 🚀 설치 및 사용 방법

1. **배포**:
   이 저장소의 `dags/` 디렉토리 내에 있는 Python 파일들을 Airflow 환경의 `dags/` 폴더로 복사하거나 배포합니다.

2. **설정**:
   별도의 추가 설정은 필요하지 않으나, 각 DAG 파일 상단의 설정 변수(Threshold 등)를 운영 환경에 맞게 조정할 수 있습니다.
   * 예: `LONG_RUNNING_THRESHOLD`, `STUCK_QUEUED_THRESHOLD` 등

3. **확인**:
   Airflow UI에서 각 DAG을 활성화(Unpause)한 후, 실행 로그(Task Log)를 통해 수집된 정보를 확인할 수 있습니다.
   * 추후 Slack 알림이나 DB 적재 로직을 추가하여 확장할 수 있습니다.

## 🛠 성능 최적화
모든 DAG은 운영 DB에 부하를 주지 않도록 최적화되어 있습니다.
- `DagBag` 파싱 대신 DB 메타데이터 직접 조회.
- N+1 쿼리 방지를 위한 Bulk 조회 및 메모리 매핑 적용.

## 👤 작성자
* **Author**: jsangmin <jsm20up@gmail.com>
