package com.guyue.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @ClassName aaaa
 * @Description TOOD
 * @Author lipeng
 */
public class CommonTestUDF extends ScalarFunction {

    private int factor = 12;

    public CommonTestUDF() {
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
        if (StringUtils.isEmpty(s)) {
            return -1;
        }
        return s.hashCode();
    }

    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.INT;
    }
}
