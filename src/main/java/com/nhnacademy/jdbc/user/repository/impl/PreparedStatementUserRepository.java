package com.nhnacademy.jdbc.user.repository.impl;

import com.nhnacademy.jdbc.user.domain.User;
import com.nhnacademy.jdbc.user.repository.UserRepository;
import com.nhnacademy.jdbc.util.DbUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.Optional;

@Slf4j
public class PreparedStatementUserRepository implements UserRepository {
    @Override
    public Optional<User> findByUserIdAndUserPassword(String userId, String userPassword) {
        //todo#11 -PreparedStatement- 아이디 , 비밀번호가 일치하는 회원조회

        String sql = "select * from jdbc_users where user_id = ? and user_password = ?";

        Connection connection = null;

        try {
            connection = DbUtils.getConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, userId);
            stmt.setString(2, userPassword);
            ResultSet rs = stmt.executeQuery();

            if(rs.next()) {
                User user = new User(
                        rs.getString("user_id"),
                        rs.getString("user_name"),
                        rs.getString("user_password")
                );

                return Optional.of(user);
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        return Optional.empty();
    }

    @Override
    public Optional<User> findById(String userId) {
        //todo#12-PreparedStatement-회원조회
        String sql = "select * from jdbc_users where user_id = ?";

        Connection connection = null;

        try {
            connection = DbUtils.getConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, userId);
            ResultSet rs = stmt.executeQuery();

            if(rs.next()) {
                User user = new User(
                        rs.getString("user_id"),
                        rs.getString("user_name"),
                        rs.getString("user_password")
                );

                return Optional.of(user);
            }


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        return Optional.empty();
    }

    @Override
    public int save(User user) {
        //todo#13-PreparedStatement-회원저장

        String sql = "insert into jdbc_users values (?, ?, ?)";

        Connection connection = null;

        try {
            connection = DbUtils.getConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, user.getUserId());
            stmt.setString(2, user.getUserName());
            stmt.setString(3, user.getUserPassword());
            int rs = stmt.executeUpdate();

            return rs;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public int updateUserPasswordByUserId(String userId, String userPassword) {
        //todo#14-PreparedStatement-회원정보 수정
        String sql = "update jdbc_users set user_password = ? where user_id = ?";

        Connection connection = null;

        try {
            connection = DbUtils.getConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, userPassword);
            stmt.setString(2, userId);
            int rs = stmt.executeUpdate();

            return rs;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int deleteByUserId(String userId) {
        //todo#15-PreparedStatement-회원삭제
        String sql = "delete from jdbc_users where user_id = ?";

        Connection connection = null;

        try {
            connection = DbUtils.getConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            stmt.setString(1, userId);
            int rs = stmt.executeUpdate();

            return rs;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
