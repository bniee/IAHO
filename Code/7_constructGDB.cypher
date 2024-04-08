## 사고사례 노드 생성
LOAD CSV WITH HEADERS FROM 'file:///hazardDB_231024.csv' AS row
merge (n:case {contype:row.conType, accident:row.accident, accObject:row.accObject, process:row.process, cause:row.cause});

==========================================================
## 빈도 및 강도 노드 생성 (risk1: 공종-사고유형)
LOAD CSV WITH HEADERS FROM 'file:///hazardDB_231024.csv' AS row
merge (r:risk1 {contype:row.conType, accident:row.accident, frq1:toFloat(coalesce(row.frq1, '0')), str1:toFloat(coalesce(row.str1, '0'))});

MATCH (n:case)
WHERE n.contype is not null
merge (t:conType {conType: n.contype});

MATCH (n:case)
WHERE n.accident is not null
merge (a:accident {accident: n.accident, conType: n.contype});

match (n:conType),(a:accident), (r1:risk1)
where n.conType = a.conType = r1.contype and a.accident=r1.accident
merge (n) <- [w:within {frq: toFloat(r1.frq1), str: toFloat(r1.str1)}]- (a);

match (r1) delete r1;
----------------------------------------------------
## 빈도 및 강도 노드 생성 (risk2: 사고유형-작업프로세스)
LOAD CSV WITH HEADERS FROM 'file:///hazardDB_231024.csv' AS row
merge (r:risk2 {accident:row.accident, process: row.process, frq2:toFloat(coalesce(row.frq2, '0')), str2:toFloat(coalesce(row.str2, '0'))});

MATCH (n:case)
WHERE n.process is not null
merge (p:process {process: n.process, conType: n.contype});

match (n:case), (p:process), (a:accident), (r2:risk2)
where n.contype = a.conType = p.conType and n.accident = a.accident = r2.accident and n.process = p.process = r2.process
merge (p) - [d:during {frq: toFloat(r2.frq2), str: toFloat(r2.str2)}] -> (a);

match (r2) delete r2;
-------------------------------------------------------
## 빈도 및 강도 노드 생성 (risk3: 작업프로세스-사고객체)
LOAD CSV WITH HEADERS FROM 'file:///hazardDB_231024.csv' AS row
merge (r:risk3 {process: row.process, accObject: row.accObject, frq3:toFloat(coalesce(row.frq3, '0')), str3:toFloat(coalesce(row.str3, '0'))});

MATCH (n:case)
WHERE n.accObject is not null
merge (o:accObject {accObject: n.accObject,conType: n.contype});

match (n:case), (p:process), (a:accident), (o:accObject), (r3:risk3)
where n.contype = a.conType = p.conType = o.conType and n.accident = a.accident and n.process = p.process = r3.process and n.accObject = o.accObject = r3.accObject
merge (o) - [t:at {frq: toFloat(r3.frq3), str: toFloat(r3.str3)}] -> (p);

match (r3) delete r3;
---------------------------------------------------------
## 빈도 및 강도 노드 생성 (risk4: 사고객체-주원인)
LOAD CSV WITH HEADERS FROM 'file:///hazardDB_231024.csv' AS row
merge (r:risk4 {accObject: row.accObject, cause: row.cause, frq4:toFloat(coalesce(row.frq4, '0')), str4:toFloat(coalesce(row.str4, '0'))});

MATCH (n:case)
WHERE n.cause is not null
merge (c:cause {cause: n.cause,conType: n.contype});

match (n:case), (p:process), (a:accident), (o:accObject), (c:cause), (r4:risk4)
where n.contype = a.conType = p.conType = o.conType = c.conType and n.accident = a.accident and n.process = p.process and n.accObject = o.accObject = r4.accObject and n.cause = c.cause = r4.cause
merge (c) - [b:by {frq: toFloat(r4.frq4), str: toFloat(r4.str4)}] -> (o);

match (r4) delete r4;

match (n:case) delete n;
==========================================================
## 시나리오 위험도 계산
MATCH (n:conType) <- [w:within] - (a:accident)
MATCH (a:accident) <- [d:during] - (p:process)
MATCH (p:process) <- [t:at] - (o:accObject)
MATCH (o:accObject) <- [b:by] - (c:cause)
RETURN
n.conType AS conType,
a.accident AS accident,
p.process AS process,
o.accObject AS object,
c.cause AS cause,
REDUCE(s = 1, r IN COLLECT({frq: w.frq+d.frq+t.frq+b.frq, str: w.str+d.str+t.str+b.str}) | r.frq + r.str) AS calculatedRisk

MATCH (n:conType) <- [w:within] - (a:accident) <- [d:during] - (p:process) <- [t:at] - (o:accObject) <- [b:by] - (c:cause)

REDUCE(s = 1, r IN COLLECT({frq: COALESCE(w.frq,'0')+COALESCE(d.frq,'0')+COALESCE(t.frq,'0')+COALESCE(b.frq,'0'), str: w.str+d.str+t.str+b.str}) | r.frq + r.str) AS calculatedRisk

## 단일 위험도 계산
match (n:conType) <- [w:within] - (a:accident) return n.conType, a.accident, w.frq, w.str, w.frq * w.str as calculatedRisk
==========================================================
## 특정 공종의 사고시나리오 검색하기
match (t:conType {conType:'토공사'}) <- [r1:occured] - (a:accident) <- [r2:workProcess] - (p:process) <- [r3:accObject]- (o:accObject) <- [r4:causedBy] - (c:cause)
return t,a,p,o,c,r1,r2,r3,r4 limit 5000

## 특정 사고유형에 해당하는 공종과 작업프로세스 검색하기
match (a:accident {accident:'물체에 맞음'}) - [r1:occured] -> (t:conType),(a:accident {accident:'물체에 맞음'}) <- [r2:workProcess] - (p:process) 
return a,r1,t,r2,p limit 5000

match (p:process) - [r2:workProcess] -> (a:accident) - [r1:occured] -> (t:conType) where a.accident starts with '넘어짐' 
return a,r1,r2,t,p limit 5000

match (c:cause) - [r1] -> (o:accObject) - [r2] ->(p:process) - [r3] -> (a:accident) - [r4] -> (t:conType) where a.accident starts with '넘어짐' and c.cause = '작업자 부주의' 
return c,r1,o,r2,p,r3,a,r4,t limit 5000

match (c:cause) - [r1] -> (o:accObject) - [r2] ->(p:process) - [r3] -> (a:accident) - [r4] -> (t:conType) where c.cause = '작업자 부주의' 
return c,r1,o,r2,p,r3,a,r4,t limit 5000
==========================================================
## 모든 관계 삭제
MATCH ()-[r]-()
DELETE r

## 모든 노드 삭제
MATCH (n)
DELETE n