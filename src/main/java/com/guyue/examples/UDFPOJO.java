package com.guyue.examples;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * @ClassName UDFPOJO
 * @Description TOOD
 * @Author lipeng
 * @Date 2019/7/1 17:25
 */
public class UDFPOJO implements Serializable {
    private List<String> methodName;
    private List<String> classPath;
    private String       jarPath;

    public UDFPOJO() {
    }

    public UDFPOJO(List<String> methodName,
                   List<String> classPath,
                   String jarPath) {
        this.methodName = methodName;
        this.classPath = classPath;
        this.jarPath = jarPath;
    }

    public List<String> getMethodName() {
        return methodName;
    }

    public void setMethodName(List<String> methodName) {
        this.methodName = methodName;
    }

    public List<String> getClassPath() {
        return classPath;
    }

    public void setClassPath(List<String> classPath) {
        this.classPath = classPath;
    }

    public String getJarPath() {
        return jarPath;
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UDFPOJO udfEntity = (UDFPOJO) o;
        return Objects.equals(methodName, udfEntity.methodName) &&
                Objects.equals(classPath, udfEntity.classPath) &&
                Objects.equals(jarPath, udfEntity.jarPath);
    }

    @Override
    public int hashCode() {

        return Objects.hash(methodName, classPath, jarPath);
    }
}
