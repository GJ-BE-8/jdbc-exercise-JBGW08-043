package com.nhnacademy.jdbc.student.repository.impl;

import com.nhnacademy.jdbc.student.domain.Student;
import com.nhnacademy.jdbc.student.repository.StudentRepository;
import com.nhnacademy.jdbc.util.DbUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.Optional;

@Slf4j
public class PreparedStatementStudentRepository implements StudentRepository {

    @Override
    public int save(Student student){
        //todo#1 학생 등록

        String sql = "insert into jdbc_students(id, name, gender, age) values(?, ?, ?, ?)";

        Connection connection = null;

        try {
            connection = DbUtils.getConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, student.getId());
            stmt.setString(2, student.getName());
            stmt.setString(3, student.getGender().toString());
            stmt.setInt(4, student.getAge());
            return stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Student> findById(String id){
        //todo#2 학생 조회

        String sql = "select * from jdbc_students where id = ?";

        Connection connection = null;

        try {
            connection = DbUtils.getConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, id);
            ResultSet rs = stmt.executeQuery();

            if(rs.next()){
                Student student = new Student(
                        rs.getString("id"),
                        rs.getString("name"),
                        Student.GENDER.valueOf(rs.getString("gender")),
                        rs.getInt("age"),
                        rs.getTimestamp("created_at").toLocalDateTime()
                );

                return Optional.of(student);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return Optional.empty();
    }

    @Override
    public int update(Student student){
        //todo#3 학생 수정 , name 수정

        String sql = "update jdbc_students set name = ?, gender = ?, age = ? where id = ?";

        Connection connection = null;

        try {
            connection = DbUtils.getConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, student.getName());
            stmt.setString(2, student.getGender().toString());
            stmt.setInt(3, student.getAge());
            stmt.setString(4, student.getId());
            return stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int deleteById(String id){
        //todo#4 학생 삭제

        String sql = "delete from jdbc_students where id = ?";

        Connection connection = null;

        try {
            connection = DbUtils.getConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, id);
            return stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

}
