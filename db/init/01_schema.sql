-- 水质样品表：样品号、站号、pH
CREATE TABLE IF NOT EXISTS public.samples (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sample_no   text NOT NULL UNIQUE,                 -- 样品号
    station_no  text NOT NULL,                        -- 站号
    ph          numeric(4,2) NOT NULL CHECK (ph BETWEEN 0 AND 14),
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_samples_station ON public.samples (station_no);

-- 站号归并映射：设备更名后，旧站号 → 现行站号
-- 同一旧号只能登记一次（主键），不可改指他站；连续更名以链式记录，
-- 环路由 API 登记时拒绝。映射随基准备份与 WAL 进入 PITR，
-- 恢复实例里看到的映射即核对点时点的映射；老核对点可能没有本表，
-- 查询需回退为按原站号供数。
CREATE TABLE IF NOT EXISTS public.station_aliases (
    old_station_no  text PRIMARY KEY,               -- 被归并的旧站号
    new_station_no  text NOT NULL,                  -- 归并目标（现行站号或链中下一棒）
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- 预置样品数据
INSERT INTO public.samples (sample_no, station_no, ph) VALUES
    ('S-2026-0001', 'ST-01', 7.10),
    ('S-2026-0002', 'ST-01', 6.95),
    ('S-2026-0003', 'ST-02', 7.40);
