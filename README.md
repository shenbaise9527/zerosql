# zerosql
利用[gorm](https://gorm.io/)来实现[go-zero](https://github.com/tal-tech/go-zero)中的`sqlx.SqlConn`接口,把`gorm`集成到`go-zero`中.

实现逻辑是参考`go-zero`中的`sqlx.commonSqlConn`来的.

``` go
// 创建sqlx.SqlConn对象.
conn := zerosql.NewZeroMysql(c.DataSource)

// 使用方式与sqlx.NewMySql(c.DataSource)创建出来的对象行为完全一致.
```