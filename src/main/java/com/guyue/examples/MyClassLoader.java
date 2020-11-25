package com.guyue.examples;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * @ClassName MyClassLoader
 * @Description TOOD
 * @Author lipeng
 * @Date 2019/7/2 11:40
 */
public class MyClassLoader extends URLClassLoader {

    public final static String CLASS_FILE_SUFFIX = ".class";

    private ClassLoader parents;

    public MyClassLoader(URL[] urls) {
        super(urls);
    }

    public MyClassLoader(URL[] urls,
                         ClassLoader parent) {
        super(urls, parent);
        this.parents = parent;
    }

    @Override
    public Class<?> loadClass(final String name) throws ClassNotFoundException {
        return this.loadClass(name, false);
    }

    @Override
    public Class<?> loadClass(final String name,
                              final boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> clazz = null;

            // 0. check the class whether was loaded.
            clazz = findLoadedClass(name);
            if (null != clazz) {
                if (resolve) {
                    resolveClass(clazz);
                }
                return clazz;
            }

            // 1. findClass
            try {
                clazz = findClass(name);
                if (null != clazz) {
                    if (resolve) {
                        resolveClass(clazz);
                    }
                    return clazz;
                }
            } catch (ClassNotFoundException e) {
                // ignore
            }

            // 2. delegating to parent classloader at end.
            try {
                clazz = Class.forName(name, false, parents);
                if (null != clazz) {
                    if (resolve) {
                        resolveClass(clazz);
                    }
                    return clazz;
                }
            } catch (ClassNotFoundException e) {
                //
            }

        }
        throw new ClassNotFoundException(name);
    }

    @Override
    public void addURL(URL url) {
        super.addURL(url);
    }
}
