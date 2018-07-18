package org.zach;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * User: 过往记忆
 * Date: 2015-08-05
 * Time: 下午23:49
 * bolg: https://www.iteblog.com
 * 本文地址：https://www.iteblog.com/archives/1415
 * 过往记忆博客，专注于hadoop、hive、spark、shark、flume的技术博客，大量的干货
 * 过往记忆博客微信公共帐号：iteblog_hadoop
 */
public class EntryGroupingComparator extends WritableComparator {
    public EntryGroupingComparator() {
        super(Entry.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Entry a1 = (Entry) a;
        Entry b1 = (Entry) b;
        return a1.getYearMonth().compareTo(b1.getYearMonth());
    }
}
