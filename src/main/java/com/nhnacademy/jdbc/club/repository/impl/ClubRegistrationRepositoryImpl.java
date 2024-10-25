package com.nhnacademy.jdbc.club.repository.impl;

import com.nhnacademy.jdbc.club.domain.Club;
import com.nhnacademy.jdbc.club.domain.ClubStudent;
import com.nhnacademy.jdbc.club.repository.ClubRegistrationRepository;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;

@Slf4j
public class ClubRegistrationRepositoryImpl implements ClubRegistrationRepository {

    @Override
    public int save(Connection connection, String studentId, String clubId) {
        //todo#11 - 핵생 -> 클럽 등록, executeUpdate() 결과를 반환
        String sql = "insert into jdbc_club_registrations values (?, ?)";

        try {
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, studentId );
            stmt.setString(2, clubId );
            return stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int deleteByStudentIdAndClubId(Connection connection, String studentId, String clubId) {
        //todo#12 - 핵생 -> 클럽 탈퇴, executeUpdate() 결과를 반환
        String sql = "delete from jdbc_club_registrations where student_id = ? and club_id = ?";

        try {
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, studentId );
            stmt.setString(2, clubId );
            return stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ClubStudent> findClubStudentsByStudentId(Connection connection, String studentId) {
        //todo#13 - 핵생 -> 클럽 등록, executeUpdate() 결과를 반환

        String sql =
                "select * "+
                        "from jdbc_club_registrations cr "+
                        "inner join jdbc_club c on cr.club_id = c.club_id "+
                        "inner join jdbc_students s on cr.student_id = s.id "+
                        "where id = ?";

        ResultSet rs = null;
        try {
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, studentId );
            rs = stmt.executeQuery();
            List<ClubStudent> csList = new ArrayList<>();

            while(rs.next()){
                ClubStudent cs = new ClubStudent(
                        rs.getString("id"),
                        rs.getString("name"),
                        rs.getString("club_id"),
                        rs.getString("club_name")
                );

                csList.add(cs);
            }
                return csList;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if(rs != null){
                    rs.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public List<ClubStudent> findClubStudents(Connection connection) {
        //todo#21 - join
        String sql =
                "select "+
                    "a.id as student_id,"+
                    "a.name as student_name,"+
                    "c.club_id,"+
                    "c.club_name "+
                "from jdbc_students a "+
                    "inner join jdbc_club_registrations b on a.id=b.student_id "+
                    "inner join jdbc_club c on b.club_id=c.club_id "+
                "order by a.id asc, b.club_id asc";

        return findClubStudents(connection, sql);

    }

    @Override
    public List<ClubStudent> findClubStudents_left_join(Connection connection) {
        //todo#22 - left join

        String sql =
                "select " +
                        "a.id as student_id, " +
                        "a.name as student_name, " +
                        "c.club_id, " +
                        "c.club_name " +
                        "from jdbc_students a " +
                        "left join jdbc_club_registrations b on a.id = b.student_id " +
                        "left join jdbc_club c on b.club_id = c.club_id " +
                        "order by a.id asc, b.club_id asc";

        return findClubStudents(connection, sql);

    }

    @Override
    public List<ClubStudent> findClubStudents_right_join(Connection connection) {
        //todo#23 - right join
        String sql =
                "select " +
                        "a.id as student_id, " +
                        "a.name as student_name, " +
                        "c.club_id, " +
                        "c.club_name " +
                        "from jdbc_students a " +
                        "right join jdbc_club_registrations b on a.id = b.student_id " +
                        "right join jdbc_club c on b.club_id = c.club_id " +
                        "order by c.club_id asc, a.id asc";

        return findClubStudents(connection, sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_full_join(Connection connection) {
        //todo#24 - full join = left join union right join

        StringBuilder sb = new StringBuilder();

        sb.append("select " +
                "a.id as student_id, " +
                "a.name as student_name, " +
                "c.club_id, " +
                "c.club_name " +
                "from jdbc_students a " +
                "left join jdbc_club_registrations b on a.id = b.student_id " +
                "left join jdbc_club c on b.club_id = c.club_id");
        sb.append("\nunion\n");
        sb.append("select " +
                "a.id as student_id, " +
                "a.name as student_name, " +
                "c.club_id, " +
                "c.club_name " +
                "from jdbc_students a " +
                "right join jdbc_club_registrations b on a.id = b.student_id " +
                "right join jdbc_club c on b.club_id = c.club_id ");


        return findClubStudents(connection, sb.toString());
    }

    @Override
    public List<ClubStudent> findClubStudents_left_excluding_join(Connection connection) {
        //todo#25 - left excluding join
        String sql =
                "select " +
                        "a.id as student_id, " +
                        "a.name as student_name, " +
                        "c.club_id, " +
                        "c.club_name " +
                        "from jdbc_students a " +
                        "left join jdbc_club_registrations b on a.id = b.student_id " +
                        "left join jdbc_club c on b.club_id = c.club_id " +
                        "where c.club_id is null " +
                        "order by a.id asc";

        return findClubStudents(connection, sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_right_excluding_join(Connection connection) {
        //todo#26 - right excluding join
        String sql =
                "select " +
                        "a.id as student_id, " +
                        "a.name as student_name, " +
                        "c.club_id, " +
                        "c.club_name " +
                        "from jdbc_students a " +
                        "right join jdbc_club_registrations b on a.id = b.student_id " +
                        "right join jdbc_club c on b.club_id = c.club_id " +
                        "where a.id is null " +
                        "order by b.club_id asc";

        return findClubStudents(connection, sql);
    }

    @Override
    public List<ClubStudent> findClubStudents_outher_excluding_join(Connection connection) {
        //todo#27 - outher_excluding_join = left excluding join union right excluding join
        StringBuilder sb = new StringBuilder();

        sb.append("select " +
                "a.id as student_id, " +
                "a.name as student_name, " +
                "c.club_id, " +
                "c.club_name " +
                "from jdbc_students a " +
                "left join jdbc_club_registrations b on a.id = b.student_id " +
                "left join jdbc_club c on b.club_id = c.club_id " +
                "where c.club_id is null ");
        sb.append("\nunion\n");
        sb.append("select " +
                "a.id as student_id, " +
                "a.name as student_name, " +
                "c.club_id, " +
                "c.club_name " +
                "from jdbc_students a " +
                "right join jdbc_club_registrations b on a.id = b.student_id " +
                "right join jdbc_club c on b.club_id = c.club_id " +
                "where a.id is null ");


        return findClubStudents(connection, sb.toString());
    }

    private List<ClubStudent> findClubStudents(Connection connection, String sql){

        ResultSet rs = null;
        try {
            PreparedStatement stmt = connection.prepareStatement(sql);
            rs = stmt.executeQuery();
            List<ClubStudent> csList = new ArrayList<>();

            while(rs.next()){
                ClubStudent cs = new ClubStudent(
                        rs.getString("student_id"),
                        rs.getString("student_name"),
                        rs.getString("club_id"),
                        rs.getString("club_name")
                );

                csList.add(cs);
            }
            return csList;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if(rs != null){
                    rs.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

}