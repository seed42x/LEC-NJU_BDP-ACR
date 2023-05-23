package org.example.knn;

public class KNNDistCalculator {
    public static double EuclideanDist(Double[] a, Double[] b) {
        /**
         * Calculate Euclidean Distance : dist = sqrt((x1 - y1)^2 + (x2 - y2)^2 + ... + (xn-yn)^2)
         */
        double rst = 0.0;
        for(int i = 0; i < a.length; i++) {
            rst += Math.pow(a[i] - b[i], 2);
        }
        return Math.sqrt(rst);
    }

    public static double EuclideanWeight(double dist) {
        /**
         * @Input: a classic Euclidean Distance
         * @Output: a weight about the input dist
         * @Note:
         *  weight = 1 / (dist + const)
         *  The const avoid this case: dist->0, weight-> infinity
         */
        double const_val = 1.0;
        return 1 / (dist + const_val);
    }

    public static double GaussianWeight(double dist) {
        /**
         * Input a distance and return its weight
         */
        double sigma=10.0; //set to 10.0
        return Math.exp(-Math.pow(dist, 2) / (2 * Math.pow(sigma, 2)));
    }
}

