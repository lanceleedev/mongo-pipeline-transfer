package com.mongodb.pipeline.transfer.helper;

import com.mongodb.pipeline.transfer.constants.StagesConstants;
import com.mongodb.pipeline.transfer.parse.stage.AddFieldsParse;
import com.mongodb.pipeline.transfer.parse.stage.GroupParse;
import com.mongodb.pipeline.transfer.parse.stage.LookupParse;
import com.mongodb.pipeline.transfer.parse.stage.MatchParse;
import com.mongodb.pipeline.transfer.parse.stage.ProjectParse;
import com.mongodb.pipeline.transfer.parse.stage.UnwindParse;
import org.bson.conversions.Bson;

/**
 * Stage helper
 * <pre>
 * Modify Information:
 * Author       Date          Description
 * ============ ============= ============================
 * lilei        2019/9/16     Create this file
 * </pre>
 */
public final class StageHelper {
    private StageHelper() {
    }

    /**
     * <p>解析stage</p>
     *
     * @param stage 阶段
     * @param value 解析内容
     * @return
     */
    public static Bson parse(String stage, String value) {
        Bson bson = null;
        switch (stage) {
            case StagesConstants.ADD_FIELDS:
                bson = AddFieldsParse.process(value);
                break;
            case StagesConstants.GROUP:
                bson = GroupParse.process(value);
                break;
            case StagesConstants.LOOKUP:
                bson = LookupParse.process(value);
                break;
            case StagesConstants.MATCH:
                bson = MatchParse.process(value);
                break;
            case StagesConstants.PROJECT:
                bson = ProjectParse.process(value);
                break;
            case StagesConstants.UNWIND:
                bson = UnwindParse.process(value);
                break;
            default:
                throw new RuntimeException("Dont't support this pipline stage!" + stage);
        }
        return bson;
    }
}
