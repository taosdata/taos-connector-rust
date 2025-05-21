adapter log:
poll:
cat taosadapter_32_20250520.log | grep "call tmq_poll" |       grep "05/19 11:29" | wc -l
cat taosadapter_32_20250520.log | grep "tmq_poll finish" |     grep "05/19 11:29" | wc -l
cat taosadapter_32_20250520.log | grep "tmq_poll finish" |     grep "05/19 11:29" | grep -v "res:0x0" | wc -l
cat taosadapter_32_20250520.log | grep '"have_message":true' | grep "05/19 11:29" | wc -l

fetch_raw_data:
cat taosadapter_32_20250520.log | grep "get ws message data" | grep "fetch_raw_data" | wc -l
cat taosadapter_32_20250520.log | grep "write binary done, action:fetch_raw_data" | wc -l

rust log:
cat log21 | grep "send message" | grep "poll" | wc -l
cat log21 | grep "json response:" | grep '"action":"poll"' | wc -l
cat log21 | grep "poll data, data:" | wc -l
cat log21 | grep "recv data: Poll(TmqPoll" | wc -l
cat log21 | grep "Got message1 in" | wc -l
cat log21 | grep "poll data, data:" | grep "have_message: true" | wc -l
cat log21 | grep "Got message2 in" | wc -l

cat log14 | grep "send_recv, msgasdfasdfasdf: Poll" | wc -l

cat log13 | grep "jkll recv_timeout, timeout:" | wc -l
cat log13 | grep "asdf poll_wait, auto_commit" | wc -l
cat log13 | grep "ffffsdf poll timeout:" | wc -l
cat log13 | grep "ffffsdf poll timed out" | wc -l
cat log13 | grep "ffffsdf poll message:" | wc -l

cat log11 | grep "aaaa send_recv, req_id:" | wc -l
cat log11 | grep "bbbb send_recv, req_id:" | wc -l
cat log11 | grep "recv data:" | wc -l

cat log11 | grep "poll aaaa:" | wc -l
cat log11 | grep "poll bbbb, ok:" | wc -l
cat log11 | grep "poll eeee" | wc -l
cat log11 | grep "poll cccc" | wc -l
cat log11 | grep "poll dddd" | wc -l

taosx log:
cat log13 | grep "poll data start"
cat log13 | grep "poll data end"
cat log13 | grep "write data start"
cat log13 | grep "write data end"

cat log13 | grep "poll data:" | wc -l
cat log13 | grep "block_cnt" | wc -l
cat log13 | grep "parse_message, msg:" | wc -l
cat log13 | grep "poll recv_timeout, res:" | wc -l
cat log13 | grep "poll recv_timeout ok" | wc -l
cat log13 | grep "No message received" | wc -l

sql:
select count(*) from td34829_src.meters;
select count(*) from td34829_dst.meters;
select _wstart,_wend,count(*) from td34829_src111.meters interval(5s);
select _wstart,_wend,count(*) from td34829_dst111.meters interval(5s);

select * from td34829_dst111.meters where ts >= ' 2025-05-19 18:08:40.000' and ts <= '2025-05-19 18:08:45.000';

awk '{
    for (i = 1; i <= NF; i++) {
        if ($i == "poll") {
            if ((i + 2) <= NF && $(i + 1) == "rows:") {
                rows += $(i + 2)
            }
        } else if ($i == "block_cnt:") {
            if ((i + 1) <= NF) {
                blocks += $(i + 1)
            }
        }
    }
}
END {
    print "Total poll rows:", rows
    print "Total block_cnt:", blocks
}' log13

cargo nextest run test_td34829_with_taos --nocapture --retries 0 > ../log/log14 2>&1
