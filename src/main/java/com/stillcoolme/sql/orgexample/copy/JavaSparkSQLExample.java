package com.stillcoolme.sql.orgexample.copy;

import java.io.Serializable;

/**
 * Created by zhangjianhua on 2018/9/17.
 */
public class JavaSparkSQLExample {

    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }


    public static void main(String[] arg){



    }
}
