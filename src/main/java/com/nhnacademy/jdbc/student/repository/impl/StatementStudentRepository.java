package com.nhnacademy.jdbc.student.repository.impl;

import com.nhnacademy.jdbc.student.domain.Student;
import com.nhnacademy.jdbc.student.repository.StudentRepository;
import com.nhnacademy.jdbc.util.DbUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class StatementStudentRepository implements StudentRepository {

    @Override
    public int save(Student student){
        //todo#1 insert student
        String sql = String.format(
                "insert into jdbc_students(id, name, gender, age) values('%s', '%s', '%s', %d)",
                student.getId(),
                student.getName(),
                student.getGender(),
                student.getAge()
                );

        log.debug("save sql : {}", sql);

        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:mysql://133.186.241.167:3306/nhn_academy_43","nhn_academy_43","nBsX6gXJ7loHtMn/");
            Statement statement = connection.createStatement();
            int result = statement.executeUpdate(sql);

            return result;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Optional<Student> findById(String id){
        //todo#2 student 조회

        String sql = String.format("select * from jdbc_students where id = '%s'",
                id);

        log.debug("findById sql : {}", sql);

        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:mysql://133.186.241.167:3306/nhn_academy_43","nhn_academy_43","nBsX6gXJ7loHtMn/");
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);

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
        //todo#3 student 수정, name <- 수정합니다.

        String sql = String.format("update jdbc_students set name = '%s', gender = '%s', age = %d where id = '%s'",
                student.getName(),
                student.getGender(),
                student.getAge(),
                student.getId()
        );

        log.debug("update sql: {}", sql);

        Connection connection = null;

        try {
            connection = DriverManager.getConnection("jdbc:mysql://133.186.241.167:3306/nhn_academy_43","nhn_academy_43","nBsX6gXJ7loHtMn/");
            Statement statement = connection.createStatement();
            int rs = statement.executeUpdate(sql);

            log.debug("update 결과: {}", rs);
            return rs;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public int deleteById(String id){
       //todo#4 student 삭제

        String sql = String.format(
                "delete from jdbc_students where id = '%s'",
                id);

        Connection connection = null;

        try {
            connection = DriverManager.getConnection("jdbc:mysql://133.186.241.167:3306/nhn_academy_43","nhn_academy_43","nBsX6gXJ7loHtMn/");
            Statement statement = connection.createStatement();
            int rs = statement.executeUpdate(sql);

            log.debug("delete 결과: {}", rs);
            return rs;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

}
