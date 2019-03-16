package com.stillcoolme.sql.gxx;

/**
 * 根据车牌找出最大时间的车辆信息。
 *
 * 问题：
 * 假如dataset中包括user，time，detail，如何能最快的根据user找出最大time的detail？
 * 我的思路比较简单就是先groupby(user).max(time)，然后重新跟原始的dataset进行join，但是感觉有点麻烦， 不知大神们是怎么处理这样的问题的
 *
 * 昔扬:
 * val ws: WindowSpec = Window.partitionBy("detail").orderBy($"time".desc)
 * df.select("","","",row_number().over(ws).as("")).where(""<=1)
 *
 * row_number() over (partition by xxxx order by xxxxx desc) as rowNum取rowNum=1的
 *
 *
 * spark.sql("select user, detail from (SELECT *, RANK() OVER (partition by user ORDER BY time desc) as rank FROM user_table) tmp where tmp.rank = 1")
 * @author stillcoolme
 * @date 2019/3/16 11:19
 */
public class CarNumTopN {



}

