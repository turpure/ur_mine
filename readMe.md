## pyppeteer使用方法和源码研究

### chrome僵尸进程解决办法
1. 同步和异步代码混用，出现意料之外的结果。浏览器退出之前，程序异常关闭。
临时解决方案：修改websocket protocol源码
```python
ping_timeout =None
ping_interval = None

```
2. 同时采集多个任务的时候，依旧会有chrome不能正常退出，要寻找解决办法。

## supervisor 使用方法


## js 加密参数破解
