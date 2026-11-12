# TP05 — Real-Time Data Streaming with Apache Kafka

**Module:** Big Data Analytics — Master 2 IA & Data Science, University of Eloued (2026/2027)

## 1. هيكل المشروع

```
tp05-kafka/
├── compose.yaml          # Kafka (KRaft) عبر Docker Compose
├── requirements.txt      # kafka-python==2.0.2
├── producer.py           # المنتِج (30 حدث)
├── consumer.py           # المستهلك + تنبيه الحرارة >= 35
├── extras/               # حلول "Tasks for Further Investigation"
│   ├── producer_extended.py   # N حدث + campus_building
│   ├── consumer_avg.py        # المتوسط المتحرك + group_id مختلف
│   ├── consumer_csv.py        # فلترة مبنى + حفظ CSV
│   └── commands.sh            # أوامر فحص Kafka
├── screenshots/          # لقطات الشاشة (الدليل)
└── README.md
```

## 2. خطوات التشغيل

```bash
# 1) تشغيل Kafka
docker compose up -d
docker compose ps
docker compose logs --tail=50 kafka

# 2) بيئة بايثون
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

# 3) إنشاء الـ topic يدويًا (3 partitions)
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic campus-sensors --partitions 3 --replication-factor 1

# 4) Terminal 2: المستهلك أولًا
python consumer.py
# 5) Terminal 1: المنتِج
python producer.py

# 6) الفحص
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-sensors
docker exec -it tp05-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# 7) التنظيف
docker compose down        # إيقاف فقط
docker compose down -v     # + حذف البيانات
```

> ملاحظة: في نص التمرين تظهر `campussensors` في أمر `--describe` بسبب انقسام السطر في الـ PDF، والاسم الصحيح هو `campus-sensors`.

## 3. شرح الكود باختصار

**compose.yaml**: حاوية واحدة تعمل كـ broker وcontroller معًا (`KAFKA_PROCESS_ROLES`) بوضع KRaft بدون ZooKeeper. `ADVERTISED_LISTENERS=localhost:9092` هو العنوان الذي يعطيه Kafka للعملاء. `KAFKA_NUM_PARTITIONS=3` عدد الأقسام الافتراضي للـ topics الجديدة. عوامل التكرار = 1 لأن لدينا broker واحد فقط. volume `kafka_data` يحفظ البيانات بعد إعادة التشغيل.

**producer.py**: يحوّل كل حدث إلى JSON ثم bytes (`value_serializer`)، `acks="all"` ينتظر تأكيد كل النسخ المتزامنة، يرسل حدثًا كل ثانية (30 حدثًا) ثم `flush()` لضمان إرسال ما تبقى في الذاكرة المؤقتة.

**consumer.py**: ينضم إلى المجموعة `tp05-campus-monitor`، يقرأ من البداية عند أول تشغيل (`earliest`)، يفك JSON، يطبع القياس، وينبّه إذا كانت الحرارة >= 35°C.

## 4. إجابات أسئلة التقرير

**1) ما المشكلة التي يحلها Kafka؟**
يفصل بين المنتِجين والمستهلكين (decoupling): الأحداث تُخزَّن في سجل مرتب ودائم، فيمكن لعدة تطبيقات قراءة نفس الأحداث في الوقت الحقيقي وبمعدلات مختلفة، مع قابلية التوسع والتحمل للأعطال، بدلًا من ملفات ثابتة تؤخر التحليل.

**2) التعريفات**

| المصطلح | التعريف |
|---|---|
| Producer | تطبيق يرسل (ينشر) الأحداث إلى topic |
| Consumer | تطبيق يقرأ الأحداث من topic |
| Broker | خادم Kafka الذي يخزن البيانات ويخدم الطلبات |
| Topic | قناة منطقية مسماة تُصنَّف فيها الأحداث (هنا `campus-sensors`) |
| Partition | جزء من topic، سجل مرتب لا يُعدَّل بالإضافة فقط؛ وحدة التوازي |
| Offset | رقم تسلسلي لكل رسالة داخل partition معينة |
| Consumer group | مجموعة مستهلكين بنفس `group_id` تتقاسم partitions الـ topic، وكل رسالة تُعالَج مرة واحدة داخل المجموعة |

**3) الفرق بين topic وpartition؟**
الـ topic تجميع منطقي للرسائل، والـ partition تقسيم فيزيائي له. الـ topic يتكون من partition أو أكثر؛ الترتيب مضمون داخل partition الواحدة فقط، وكل partition لها offsets خاصة بها.

**4) لماذا `auto_offset_reset='earliest'`؟ ومتى تُطبَّق؟**
ليقرأ المستهلك الرسائل من بداية السجل ولا يفقد الرسائل المرسلة قبل تشغيله. تُطبَّق **فقط** عندما لا يوجد offset محفوظ للمجموعة (مجموعة جديدة) أو يكون الـ offset المحفوظ غير صالح (محذوف بسبب الاحتفاظ). إذا وُجد offset محفوظ يكمل المستهلك منه تجاهلًا لهذا الإعداد. القيمة الأخرى `latest` تقرأ الرسائل الجديدة فقط.

**5) غرض `group_id` وماذا لو استُخدم id جديد؟**
يحدد مجموعة المستهلكين ويُحفظ باسمها الـ offset الملتزم به (committed). المستهلكون في نفس المجموعة يتقاسمون الـ partitions (توزيع الحمل). مع id جديد: تُعتبر مجموعة جديدة بلا offsets، فتبدأ حسب `auto_offset_reset` (من البداية مع earliest) وتستقبل **كل** الرسائل بشكل مستقل عن المجموعات الأخرى.

**6) لماذا JSON مناسب؟ وما حدّه؟**
مقروء للبشر، سهل التوليد والتحليل في بايثون، ولا يحتاج schema مسبقًا ومناسب للتعلم. الحد: حجمه كبير (نص مع أسماء الحقول المكررة)، أبطأ في التسلسل، ولا يفرض schema أو أنواعًا صارمة (لا رقابة على تغيّر الحقول)، لذا يُفضَّل في الإنتاج Avro أو Protobuf مع Schema Registry.

**7) كيف يبسّط Docker Compose النشر؟**
ملف واحد تصريحي يصف الخدمة والمنافذ والمتغيرات والـ volumes، فيُشغَّل كل شيء بأمر واحد (`docker compose up -d`) بدل تنزيل Kafka وضبط ملفات `server.properties` وتهيئة التخزين يدويًا. يضمن بيئة قابلة للتكرار على أي جهاز، وإيقافًا وحذفًا نظيفًا (`down`, `down -v`) دون تلويث النظام.

**8) حدود الإعداد بـ broker واحد مقارنة بالإنتاج**
- لا تحمل أعطال: سقوط الـ broker يوقف كل شيء، وreplication factor = 1 يعني عدم وجود نسخ احتياطية.
- الـ partitions لا تزيد الأداء كثيرًا لأن كل شيء على عقدة واحدة؛ التوازي الحقيقي يتطلب عدة brokers.
- لا leader election ولا ISR حقيقية، والـ controller مدمج مع الـ broker.
- بلا أمان: PLAINTEXT بدون TLS/SASL/ACL.
- بلا مراقبة وضبط للاحتفاظ والسعة ولا إعدادات إنتاج (عدة controllers، rack awareness).
- `localhost:9092` يعمل للعملاء على نفس الجهاز فقط.

**9) الأدلة (Screenshots)** — توضع في `screenshots/`:
1. `docker compose ps` (الحاوية تعمل)
2. وصف الـ topic (`--describe`)
3. مخرجات producer
4. مخرجات consumer (مع ALERT)

## 5. شرح الـ partitions والـ replication factor في هذا المعمل

مثال مخرجات `--describe` المتوقعة (الأرقام قد تختلف):

```
Topic: campus-sensors  PartitionCount: 3  ReplicationFactor: 1
  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

- **Partitions = 3**: السجل مقسّم إلى 3 أجزاء مرتبة مستقلة؛ تُوزَّع الرسائل عليها (بدون مفتاح: بالتناوب/الدُفعات). تسمح نظريًا بـ 3 مستهلكين متوازيين في مجموعة واحدة.
- **Replication factor = 1**: نسخة واحدة فقط من كل partition (على الـ broker الوحيد، رقم 1)، فلا حماية من الأعطال. القيمة القصوى الممكنة = عدد الـ brokers = 1.

## 6. حلول "Tasks for Further Investigation"

| المهمة | الحل / الملاحظة المتوقعة |
|---|---|
| 100 حدث بدل 30 | `python extras/producer_extended.py 100` — المدة أطول (نفس البنية) ويصل المستهلك 100 رسالة، ويرتفع عدد التنبيهات تناسبيًا (≈ 30% من القراءات لأن (42-35)/(42-18) ≈ 29%). |
| المتوسط المتحرك | `python extras/consumer_avg.py` — القيمة تتقارب نحو ≈ 30°C (منتصف 18–42). |
| مستهلك ثانٍ بـ group_id مختلف | `python extras/consumer_avg.py groupB` بالتوازي مع `consumer.py`: **كل** مجموعة تستقبل **كل** الرسائل (نمط pub/sub). |
| مستهلك ثانٍ في نفس المجموعة | نفس `group_id` في نافذتين: الـ partitions تُقسَّم بينهما، فتصل كل رسالة لأحدهما فقط (تقاسم الحمل). مع 3 partitions يمكن أن يتولى أحدهما اثنتين والآخر واحدة. |
| partition واحدة ثم ثلاث | **يتغير**: عدد الأقسام، وإمكانية التوازي بين المستهلكين (مع 1 مستهلك واحد فقط في المجموعة نشط)، وضمان الترتيب (شامل مع 1، لكل partition مع 3). **لا يتغير**: وجود broker واحد، عدم وجود تكرار، ومحتوى الرسائل، وعدم تحسّن الأداء الفيزيائي الحقيقي. |
| حقل `campus_building` وفلترة | `producer_extended.py` يضيف الحقل؛ `python extras/consumer_csv.py Library` يفلتر مبنى واحدًا. |
| CSV (اختياري) | `consumer_csv.py` يكتب `events.csv` بالأعمدة: event_id, sensor_id, temperature_c, humidity_pct, timestamp. |

لتغيير عدد الـ partitions:
```bash
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic campus-sensors
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic campus-sensors --partitions 1 --replication-factor 1
# ثم أعد الإنشاء بـ --partitions 3 (أو استخدم --alter --partitions 3 للزيادة فقط)
```

## 7. المشاكل المحتملة وحلولها

| المشكلة | الحل |
|---|---|
| `NoBrokersAvailable` | تأكد من `docker compose ps` وانتظر بضع ثوانٍ بعد التشغيل |
| المنفذ 9092 مشغول | غيّر `"9092:9092"` إلى مثلًا `"9094:9092"` وحدّث `bootstrap_servers` و`ADVERTISED_LISTENERS` |
| `ModuleNotFoundError: kafka.vendor.six.moves` (Python 3.12+) | `kafka-python==2.0.2` غير متوافق؛ ثبّت `kafka-python>=2.1` أو `kafka-python-ng` |
| المستهلك لا يرى رسائل قديمة | الـ group له offsets محفوظة؛ استخدم group_id جديدًا |
