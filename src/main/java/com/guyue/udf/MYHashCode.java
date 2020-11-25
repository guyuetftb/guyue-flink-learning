package com.guyue.flink.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @ClassName aaaa
 * @Description TOOD
 * @Author lipeng
 * @Date 2019/6/27 16:52
 */
public class MYHashCode extends ScalarFunction {

    private int factor = 12;

    public MYHashCode() {
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    @Override
    public void close() throws Exception {
        System.out.print("--------------------------- close()");
    }

    public int eval(String s) {
        if (null == s || s.isEmpty()) {
            return -1;
        }
        return s.hashCode();
    }

    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.INT;
    }
}
