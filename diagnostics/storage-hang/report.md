诊断完成。仓库未做任何改动，全部实验在 `/tmp/frame-proxy-diag` 隔离环境中用真实服务进程完成，可随时用 `python3 /tmp/frame-proxy-diag/run_diagnosis.py` 重跑。

# 诊断报告：存储异常后单个编号的写请求永久失去响应

## 根因（代码定位）

**`src/frame_proxy/storage/lock.py:29-34`** —— `FileKeyLock.__enter__` 在获取进程内互斥锁之后、文件锁成功之前发生异常时，互斥锁被永久泄漏：

```python
def __enter__(self) -> "FileKeyLock":
    self._path.parent.mkdir(parents=True, exist_ok=True)
    self._thread_lock.acquire()             # ① 已持有进程内锁
    self._handle = self._path.open("a+b")   # ② 存储异常在此抛出 → ① 永不释放
    fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX)  # ③ 在此抛出 → ① 同样泄漏
```

**`lock.py:36-46` 的 `__exit__` 是同构缺陷**:`fcntl.flock(LOCK_UN)` 或 `close()` 抛错（如 EIO）时，`self._thread_lock.release()` 被跳过。

泄漏的锁对象存放在模块级字典 `_THREAD_LOCKS`(lock.py:9)，按 `locks/{kind}-{id}.lock` 的解析路径作键，**永不清理、无超时、无持有者死亡检测**。于是该编号在本进程内的下一次 `FileKeyLock.__enter__` 永远阻塞在 `lock.py:31` 的 `acquire()` 上。所有写路径（`create_clip`/`create_inspection`/`update_inspection`/`create_contact_sheet`/`update_contact_sheet`/`commit_contact_sheet`,repository.py:42、90、102、127、140、162）都经过它；而所有读路径（repository.py:66、77、113、184、195）和 `/health`(router.py:59-60)**完全不取锁**——这就是"健康检查和已有对象查询仍能响应"的直接原因。

## 实验证据（隔离环境，可重复执行）

驱动脚本 `/tmp/frame-proxy-diag/run_diagnosis.py` 启动真实的 `python3 -m frame_proxy serve` 进程，客户端带 4 秒超时。完整日志在 `/tmp/frame-proxy-diag/out/run.log`。

**场景 A（纯文件系统故障，无任何注入代码）**：把 `locks/clip-clip-stuck-a.lock` 预先做成目录，`open("a+b")` 即抛出真实的 `IsADirectoryError`，模拟锁命名空间损坏/存储故障。

| 阶段 | 操作 | 结果 |
|---|---|---|
| 2 | 故障期间 `PUT /v1/clips/clip-stuck-a`（第一个失败请求） | **500 storage_failure**;GET → 404；磁盘上无 `clips/clip-stuck-a/`（什么都没发布） |
| 3 | 移除故障，`PUT clip-free-b`（无关编号） | **201** —— 存储已恢复健康 |
| 4 | 故障清除后重试 `PUT clip-stuck-a` ×2 | **均超时无响应**；线程数 1→3，两个请求线程永久驻留 |
| 5 | 卡死状态下 `/health`、GET 已有 clip、GET `clip-stuck-a`、`PUT clip-free-c` | 200 / 200 / **404** / 201 —— 读和无关编号全部正常 |
| 5 | `/proc/<pid>/task/*/wchan` | 两个卡死线程为 `futex_do_wait`（等进程内互斥锁，**不是** flock 系统调用）;`lslocks -p <pid>` 显示该进程**不持有任何文件锁** |
| 7 | SIGABRT 转储（`out/server-A.stderr.log`) | 3 个请求线程全部停在 `lock.py:31 __enter__` ← `repository.py:42 create_clip` ← `clips.py:72 admit`；主线程仍在 `serve_forever` |

**第一个失败请求如何影响后续请求**：它在 ② 处失败后把 `clip-stuck-a` 这个键的 `threading.Lock` 永远留在"已持有"状态。后续同编号写请求在 `acquire()` 上排队——**根本走不到** repository.py:43 的 `record_path.exists()` 检查，所以客户端重发得不到 409，只是无限等待。这就是"重发也不一定得到冲突"。

**场景 B(shim 在第 3 次 `flock(LOCK_EX)` 注入 EIO，即 inspection 提交 `indexed` 那一步）**：第一个 POST 本身就挂死——故障后 `_mark_failed`(inspections.py:94-107）里的 `update_inspection` 又踩中刚泄漏的锁。转储显示线程停在 `lock.py:31` ← `inspections.py:105 _mark_failed` ← `inspections.py:87 create`。此时 GET 该 inspection 返回 **200 且状态永远停在 `scanning`**:`pending`/`scanning` 记录已发布，`indexed` 永不发布，该编号再无推进可能。

**场景 D(shim 在 `__exit__` 的 `LOCK_UN` 注入 EIO)**：客户端收到 **500，但记录和媒体其实已完整提交**(GET → 200)——500 响应与持久化事实相互矛盾；重试超时而非 409。

## 三类影响的区分（实测）

- **同一进程**：同编号写请求永久挂死（场景 A 阶段 4，线程驻留、永不返回）；同编号读请求正常（404/200 照常）。
- **共享数据根的另一服务进程 B**:**完全不受影响**（场景 A 阶段 6)——B 对 `clip-stuck-a` 的 PUT 返回 201。原因：泄漏的 `threading.Lock` 是进程内对象；故障发生在 `__enter__` 时 flock 从未获得，`__exit__` 失败时被遗弃的句柄也会随异常链回收而关闭（场景 10 的 `lslocks` 证实无残留文件锁）。进程间唯一的共享事实是文件系统本身：B 提交记录后，A 的 GET 立刻能读到（200)，而 A 对同一编号的 PUT **仍然超时**——对象已存在，A 却连 409 都给不出。
- **无关编号**：锁键是 `locks/{kind}-{id}.lock`，不同编号互不干扰（`clip-free-b`、`clip-free-c`、`inspection-ok-b` 全部正常完成）。

## 对象事实的发布状态

- `__enter__` 失败：临界区从未进入，**什么都没发布**(404，无媒体、无记录、无临时文件残留）。
- 工作流中途失败（场景 B):`pending`/`scanning` 已发布且永远定格；`indexed` 永不发布。
- `__exit__` 失败（场景 D)：记录+媒体**全部已发布**，客户端却收到 500。
- 已有读取正常的原因：读路径不取锁，且 `atomic.py:33` 用 `os.replace` 原子改名发布，读者只会看到完整的新旧文件之一。

## 重启为何改变表现

`_THREAD_LOCKS` 是进程内字典，重启即清空；内核在进程退出时回收全部 flock。场景 A 阶段 8 实测：同一数据根重启后，`PUT clip-stuck-a` 从"永久挂死"变为**立即 409**（对象已被进程 B 创建），新编号 201。即重启只是绕开了泄漏的内存态，磁盘上并无任何需要清理的锁残留——这也是为什么"重启后表现又发生变化"但问题会随下一次存储异常复发。

## 次要观察

- `config.py:19` 的 `request_timeout_seconds=30` 在全代码库中未被引用，服务端没有任何请求超时兜底，挂死的线程（`daemon_threads=True`）只会无限累积。
- 若异常持续覆盖失败处理路径，`_mark_failed` 自己也会挂死（场景 B)，连"第一个请求"都不会有响应。

## 修复方向（仅说明，未改动任何业务代码）

`__enter__` 应在 ① 之后用 `try` 包裹 ②③，失败时先 `release()` 再传播；`__exit__` 应保证 `release()` 在 `finally` 中执行（flock 解锁/关闭的失败不应阻止互斥锁释放）。这样存储异常会退化为一次普通的 500，而不会把该编号在本进程内永久查封。按任务要求，我没有应用这一修改——卡住现象及其证据链保持原样，可随时复现。
