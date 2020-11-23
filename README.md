# zerosql
利用[gorm](https://gorm.io/)来实现[go-zero](https://github.com/tal-tech/go-zero)中的sqlx.SqlConn接口,把gorm集成到go-zero中.

``` go
// 创建sqlx.SqlConn对象.
conn := zerosql.NewZeroMysql(c.DataSource)

// 使用方式与sqlx.NewMySql(c.DataSource)创建出来的对象行为完全一致.
```