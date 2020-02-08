package com.example.util;

import java.util.Arrays;

public class UtilExamples {

    public static void equalPartsOfNumber(int number, int part) {
        int f = number / part;
        int r = number % part;

        int array[] = new int[part];
        for (int i = 1; i <= part; i++) {
            if (i <= part - r)
                array[i-1] = (f * i);
            else
                array[i-1] = (f * i) + r;
        }

        System.out.println(Arrays.toString(array));
    }

    public static void main(String args[]) {
        UtilExamples.equalPartsOfNumber(301, 4);
        UtilExamples.equalPartsOfNumber(301, 2);
        UtilExamples.equalPartsOfNumber(301, 1);
        UtilExamples.equalPartsOfNumber(300, 4);
        UtilExamples.equalPartsOfNumber(5, 3);
        UtilExamples.equalPartsOfNumber(8, 3);
    }
}
