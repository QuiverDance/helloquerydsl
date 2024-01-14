package study.querydsl;

import com.querydsl.core.Tuple;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceContext;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import java.util.List;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @PersistenceContext
    EntityManager em;

    JPAQueryFactory queryFactory;

    @BeforeEach
    public void before(){
        queryFactory = new JPAQueryFactory(em);

        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA); em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL(){

        em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();
    }

    @Test
    public void startQuerydsl(){
        QMember m = new QMember("m");

        queryFactory.select(m)
                .from(m)
                .where(m.username.eq("member1"))
                .fetchOne();

    }

    @Test
    public void qType(){
        queryFactory.select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();
        
        //같은 테이블을 조인해야하는 경우
        //QMember m = new QMember("m1"); 처럼 elias를 지정하여 생성
    }

    @Test
    public void search(){
        queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.between(10, 30)))
                .fetchOne();
        //and를 사용할 때에는 .and 대신 ,로 해도 동일한 결과를 얻는다.
    }

    @Test
    public void result(){
        List<Member> fetch = queryFactory
                .selectFrom(member)
                .fetch();

        Member fetchOne = queryFactory
                .selectFrom(member)
                .fetchOne();

        Member fetchFirst = queryFactory
                .selectFrom(member)
                .fetchFirst();
    }

    @Test
    public void sort(){
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();
    }

    @Test
    public void paging(){
        queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();
    }

    @Test
    public void aggregation(){
        List<Tuple> result = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();
        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
    }

    @Test
    public void group(){
        //팀의 이름과 각 팀의 평균 연령
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
    }

    @Test
    public void join(){
        List<Member> result = queryFactory
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    @Test
    public void joinOn(){
        //on절 1. 조인 대상 필터링 2. 연관관계 없는 엔티티 외부 조인
        //회원과 팀을 조인하면서 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("teamA"))
                .fetch();
        //on절을 사용하여 필터링을 할 때 leftjoin .. on 이 아닌 join .. where을 사용한 것과 결과가 동일하다.

        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));
        //회원의 이름과 팀이름이 같은 대상 외부 조인, 회원은 모두 조회
        queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team) //theta join // leftjoin(member.team, team)이 아님
                .on(member.username.eq(team.name))
                .fetch();
    }

    @PersistenceContext
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo(){
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).isFalse();
    }

    @Test
    public void fetchJoin(){
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).isTrue();
    }


    /*
    from 절의 서브 쿼리는 불가능
    해결방안
    1. 서브쿼리를 join으로 변경
    2. 애플리케이션에서 쿼리를 2번 분리해서 실행
    3. native SQL 사용
     */
    @Test
    public void subQuery(){
        QMember memberSub = new QMember("memberSub");

        //나이가 가장 많은 회원 조회
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();
        assertThat(result).extracting("age").containsExactly(40);
    }

    @Test
    public void subQueryGoe(){
        QMember memberSub = new QMember("memberSub");

        //나이가 평균 이상인 회원 조회
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();
        assertThat(result).extracting("age").containsExactly(30, 40);
    }

    @Test
    public void subQueryIn(){
        QMember memberSub = new QMember("memberSub");

        //나이가 10살 초과인 회원 조회(예시용 비효율적 쿼리)
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        JPAExpressions
                                .select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();
        assertThat(result).extracting("age").containsExactly(20, 30, 40);
    }

    @Test
    public void selectSubQuery(){
        QMember memberSub = new QMember("memberSub");
        queryFactory
                .select(member.username,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();
    }

    @Test
    public void basicCase(){
        queryFactory
                .select(member.age
                        .when(10).then("열 살")
                        .when(20).then("스물 살")
                        .otherwise("기타"))
                .from(member)
                .fetch();
    }

    @Test
    public void complexCase(){
        queryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20 살")
                        .when(member.age.between(21, 30)).then("21~30 살")
                        .otherwise("기타"))
                .from(member)
                .fetch();
    }

    @Test
    public void constant(){
        queryFactory
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();
    }

    @Test
    public void concat(){
        //{username}_{age} 형태로 가져오기
        queryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .fetch();
    }
}
