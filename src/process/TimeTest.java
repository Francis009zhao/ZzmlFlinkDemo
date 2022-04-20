package cn._51doit.flink.day07;

public class TimeTest {

    public static void main(String[] args) {

        long time = 1611111175000L;

        long res = 1611111175000L - 1611111175000L % 60000 + 60000;

        System.out.println(res);

    }
}
