package com.guyue.flink.udf;

import java.io.Serializable;
import java.net.URI;
import java.util.Objects;

/**
 * @ClassName UDFEntity
 * @Description TOOD
 * @Author lipeng
 * @Date 2019/7/1 17:25
 */
public class UDFEntity implements Serializable {
    private String methodName;
    private String classPath;
    private URI    jarPath;

    public UDFEntity() {
    }

    public UDFEntity(String methodName,
                     String classPath,
                     URI jarPath) {
        this.methodName = methodName;
        this.classPath = classPath;
        this.jarPath = jarPath;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getClassPath() {
        return classPath;
    }

    public void setClassPath(String classPath) {
        this.classPath = classPath;
    }

    public URI getJarPath() {
        return jarPath;
    }

    public void setJarPath(URI jarPath) {
        this.jarPath = jarPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UDFEntity udfEntity = (UDFEntity) o;
        return Objects.equals(methodName, udfEntity.methodName) &&
                Objects.equals(classPath, udfEntity.classPath) &&
                Objects.equals(jarPath, udfEntity.jarPath);
    }

    @Override
    public int hashCode() {

        return Objects.hash(methodName, classPath, jarPath);
    }
}
