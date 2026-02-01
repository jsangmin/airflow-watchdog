# Airflow DAG Monitor 개발 가이드

## 추천 클래스 및 패키지

기본적인 `DagBag`, `DagRun` 외에 다음 기능들을 활용하면 더욱 견고한 모니터링 시스템을 구축할 수 있습니다.

### 1. Airflow Core Models
- **`airflow.models.TaskInstance`**: 개별 Task 단위의 상태(Queued, Running, Failed), 실행 시간, 로그 URL 등을 정밀하게 모니터링할 때 필수적입니다.
- **`airflow.models.DagModel`**: DB에 저장된 DAG의 메타데이터(스케줄 간격, 활성화 여부, 마지막 실행 기록 등)를 빠르게 조회할 때 유용합니다 (`DagBag`보다 가벼움).
- **`airflow.utils.state.State`**: 상태 값(State.SUCCESS, State.FAILED 등)을 하드코딩하지 않고 관리하기 위해 사용합니다.

### 2. Event-Driven Monitoring (추천)
- **`airflow.listeners`** (Airflow 2.1+): DAG 실행, Task 실행 등의 *Lifecycle Event*를 훅(Hook)킹할 수 있는 플러그인 인터페이스입니다. 폴링(Polling) 방식보다 리소스 효율적이고 실시간성이 높습니다.
    - 예: `@hookimpl def on_task_instance_failed(...)`

### 3. API & External Integration
- **`airflow.api.client.local_client`**: Airflow 내부에서 API를 호출하여 상태를 조회하거나 Trigger할 때 사용합니다.
- **`airflow.providers.*`**: 모니터링 결과를 외부로 전송할 때 사용 (예: `airflow.providers.slack`, `airflow.providers.pagerduty`).

---

## 구현 기능 후보 (Features List)

### 1. DAG/Task 운영 모니터링
- **DAG Import Error 감지**: `DagBag.import_errors`를 주기적으로 스캔하여 문법 오류 등으로 로드되지 않은 DAG 식별.
- **Stuck/Zombie Task 감지**: `Queued` 상태 또는 `Running` 상태에서 오랫동안 멈춰 있거나 Heartbeat이 멈춘 Task 감지.
- **SLA(Service Level Agreement) 위반 알림**: DAG 완료 예정 시간을 초과하거나, 특정 중요 Task가 마감 시간 내에 끝나지 않은 경우 경고.

### 2. 리소스 및 통계
- **DAG 실행 지속 시간 추이 분석**: 최근 N번의 실행 시간을 분석하여 갑자기 느려진(Anomaly) DAG 탐지.
- **Task 실패율 분석**: 특정 기간 동안 재시도(Retry)가 빈번하거나 실패율이 높은 '불안정한' Task 리포팅.

### 3. 운영 편의성
- **Last Run Status Check**: 특정 Tag가 붙은 주요 DAG들의 최신 실행 상태를 요약하여 대시보드 형태로 제공.
- **Backfill 진행 상황 모니터링**: 과거 데이터 재처리(Backfill) 시 진행률을 시각화하거나 모니터링.
