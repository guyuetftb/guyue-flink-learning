package com.guyue.flink.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public class StationCodeToStationGroupUDF extends ScalarFunction {

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    @Override
    public void close() throws Exception {
        // TODO do nothing
    }

    public String eval(String stationCode) {
        if (StringUtils.isEmpty(stationCode)) {
            return "unknown";
        }

        return (stationCode.hashCode() % 13) + "_group";
    }

    public TypeInformation getResultType(Class<?>[] signature) {
        return Types.STRING;
    }
}
