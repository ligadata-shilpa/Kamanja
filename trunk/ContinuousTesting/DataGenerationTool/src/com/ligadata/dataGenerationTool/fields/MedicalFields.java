package com.ligadata.dataGenerationTool.fields;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class MedicalFields {


    public enum Desynpuf_Id {
        a000002F7E0A96C32,
        a00001C24EE7B06AC;

        private static final List<Desynpuf_Id> VALUES =
                Collections.unmodifiableList(Arrays.asList(values()));
        private static final int SIZE = VALUES.size();
        private static final Random RANDOM = new Random();

        public static Desynpuf_Id randomDesynput_id() {
            return VALUES.get(RANDOM.nextInt(SIZE));
        }
    }

    public enum Prvdr_Num {
        a2302KU,
        a3600CS;

        private static final List<Prvdr_Num> VALUES =
                Collections.unmodifiableList(Arrays.asList(values()));
        private static final int SIZE = VALUES.size();
        private static final Random RANDOM = new Random();

        public static Prvdr_Num randomPrvdr_num() {
            return VALUES.get(RANDOM.nextInt(SIZE));
        }
    }


}
