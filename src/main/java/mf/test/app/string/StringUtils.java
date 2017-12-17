package mf.test.app.string;

import org.apache.commons.text.similarity.LevenshteinDistance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class StringUtils {

    public static String normalizeString(String in) {
        String lowerCase = in.toLowerCase().replaceAll("[^\\w\\s]", "");
        String[] split = lowerCase.split("\\s");
        Arrays.sort(split);
        return Arrays.toString(split).replaceAll(",", "").replaceAll("[^\\w\\s]", "");
    }

    public static int countDinst(String str1, String str2) {
        List<String> str1Array = new ArrayList<>(Arrays.asList(str1.split("\\s")));
        List<String> str2Array = new ArrayList<>(Arrays.asList(str2.split("\\s")));
        rankLevel(str1Array, str2Array);
        return findOptimalResult(str1Array, str2Array);
    }

    private static int findOptimalResult(List<String> str1, List<String> str2) {
        int[][] matrix = new int[str1.size()][str2.size()];
        int i = 0;
        for (String s : str1) {
            int j = 0;
            for (String s1 : str2) {
                int res = LevenshteinDistance.getDefaultInstance().apply(s, s1);
                matrix[i][j] = res;
                j++;
            }
            i++;
        }
        return findWay(str1.size(), matrix);
    }

    private static int findWay(int size, int[][] matrix) {
        List<Integer> all = new ArrayList<>();
        for (int i = 0; i < size; i++)
            all.add(i);
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < size; i++) {
            int way = findWay(i, size, 0, matrix, all);
            if (min > way)
                min = way;
        }
        return min;
    }

    private static int findWay(int index, int size, int dep, int[][] matrix, List<Integer> map) {
        if(size-1 == dep)
            return matrix[dep][index];
        ArrayList<Integer> backup = new ArrayList<>(map);
        backup.remove(index);
        int local_min = Integer.MAX_VALUE;
        for (Integer nextIndex : backup) {
            int way = findWay(nextIndex, size, dep + 1, matrix, map) + matrix[dep][index];
            if (way < local_min)
                local_min = way;
        }
        return local_min;
    }


    private static void rankLevel(List<String> str1, List<String> str2) {
        if (str1.size() > str2.size()) {
            for (int i = 0; i < str1.size() - str2.size(); i++)
                str2.add("");
        } else {
            for (int i = 0; i < str2.size() - str1.size(); i++)
                str1.add("");
        }
    }


}
