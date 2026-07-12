package cn.javayong.shardingjdbc5.spring.common.sharding;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.RangeShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.StandardShardingAlgorithm;

import java.util.*;

/**
 * 按年份分表
 * <p>
 * 逻辑表：
 * t_ent_order_item
 * <p>
 * 实际表：
 * t_ent_order_item_2024
 * t_ent_order_item_2025
 * t_ent_order_item_2026
 * <p>
 * 分片字段：
 * create_time
 */
@Slf4j
public class YearShardingAlgorithm implements StandardShardingAlgorithm<Date> {


    @Override
    public void init(Properties props) {
        log.info("YearShardingAlgorithm init...");
    }

    /**
     * =
     * >
     * <
     * in()
     * 最终都会走这里（单值）
     */
    @Override
    public String doSharding(Collection<String> availableTargetNames,
                             PreciseShardingValue<Date> shardingValue) {

        Date date = shardingValue.getValue();

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        int year = calendar.get(Calendar.YEAR);

        String targetTable = shardingValue.getLogicTableName() + "_" + year;

        log.info("年份：{}", year);
        log.info("目标表：{}", targetTable);

        for (String table : availableTargetNames) {
            if (table.equalsIgnoreCase(targetTable)) {
                return table;
            }
        }

        throw new UnsupportedOperationException("不存在目标表：" + targetTable);
    }

    /**
     * between
     * >=
     * <=
     */
    @Override
    public Collection<String> doSharding(Collection<String> availableTargetNames,
                                         RangeShardingValue<Date> shardingValue) {

        Collection<String> result = new LinkedHashSet<>();

        Date lower = null;
        Date upper = null;

        if (shardingValue.getValueRange().hasLowerBound()) {
            lower = shardingValue.getValueRange().lowerEndpoint();
        }

        if (shardingValue.getValueRange().hasUpperBound()) {
            upper = shardingValue.getValueRange().upperEndpoint();
        }

        if (lower == null && upper == null) {
            return availableTargetNames;
        }

        Calendar calendar = Calendar.getInstance();

        int startYear;
        int endYear;

        if (lower != null) {
            calendar.setTime(lower);
            startYear = calendar.get(Calendar.YEAR);
        } else {
            startYear = Integer.MIN_VALUE;
        }

        if (upper != null) {
            calendar.setTime(upper);
            endYear = calendar.get(Calendar.YEAR);
        } else {
            endYear = Integer.MAX_VALUE;
        }

        for (String table : availableTargetNames) {

            String suffix = table.substring(table.lastIndexOf("_") + 1);

            int year = Integer.parseInt(suffix);

            if (year >= startYear && year <= endYear) {
                result.add(table);
            }
        }

        log.info("范围查询路由：{}", result);

        return result;
    }

    @Override
    public String getType() {
        return "CLASS_BASED";
    }
}