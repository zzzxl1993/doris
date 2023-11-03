// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.policy;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Save policy for storage migration.
 **/
@Data
public class InvertIndexPolicy extends Policy {
    private static final Logger LOG = LogManager.getLogger(InvertIndexPolicy.class);

    private static final String TOKENIZER = "tokenizer";
    private static final String CHAR_FILTER_TYPE = "char_filter_type";
    private static final String CHAR_FILTER_PATTERN = "char_filter_pattern";
    private static final String CHAR_FILTER_REPLACEMENT = "char_filter_replacement";
    private static final String FILTER_STOPWORDS = "filter_stopwords";
    private static final String FILTER_LOWERCASE = "filter_lowercase";

    @SerializedName(value = "tokenizer")
    private String tokenizer = null;
    @SerializedName(value = "charFilterType")
    private String charFilterType = null;
    @SerializedName(value = "charFilterPattern")
    private String charFilterPattern = null;
    @SerializedName(value = "charFilterReplacement")
    private String charFilterReplacement = null;
    @SerializedName(value = "filterStopwords")
    private String filterStopwords = null;
    @SerializedName(value = "filterLowercase")
    private String filterLowercase = null;

    public static final ShowResultSetMetaData FULLTEXT_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("PolicyName", ScalarType.createVarchar(100)))
                    .addColumn(new Column("Tokenizer", ScalarType.createVarchar(20)))
                    .addColumn(new Column("CharFilterType", ScalarType.createVarchar(20)))
                    .addColumn(new Column("CharFilterPattern", ScalarType.createVarchar(20)))
                    .addColumn(new Column("CharFilterReplacement", ScalarType.createVarchar(20)))
                    .addColumn(new Column("FilterStopwords", ScalarType.createVarchar(20)))
                    .addColumn(new Column("FilterLowercase", ScalarType.createVarchar(20)))
                    .build();

    public InvertIndexPolicy() {
        super(PolicyTypeEnum.FULLTEXT);
    }

    public InvertIndexPolicy(long policyId, final String policyName, final String tokenizer,
            final String charFilterType, final String charFilterPattern, final String charFilterReplacement,
            final String filterStopwords, final String filterLowercase) {
        super(policyId, PolicyTypeEnum.FULLTEXT, policyName);
        this.tokenizer = tokenizer;
        this.charFilterType = charFilterType;
        this.charFilterPattern = charFilterPattern;
        this.charFilterReplacement = charFilterReplacement;
        this.filterStopwords = filterStopwords;
        this.filterLowercase = filterLowercase;
    }

    public InvertIndexPolicy(long policyId, final String policyName) {
        super(policyId, PolicyTypeEnum.FULLTEXT, policyName);
    }

    public void init(final Map<String, String> props) throws AnalysisException {
        if (props == null) {
            throw new AnalysisException("properties config is required");
        }

        if (Strings.isNullOrEmpty(props.get(TOKENIZER))) {
            throw new AnalysisException("Missing [" + TOKENIZER + "] in properties.");
        }

        this.tokenizer = props.get(TOKENIZER);
        this.charFilterType = props.get(CHAR_FILTER_TYPE);
        this.charFilterPattern = props.get(CHAR_FILTER_PATTERN);
        this.charFilterReplacement = props.get(CHAR_FILTER_REPLACEMENT);
        this.filterStopwords = props.get(FILTER_STOPWORDS);
        this.filterLowercase = props.get(FILTER_LOWERCASE);
    }

    public List<String> getShowInfo() throws AnalysisException {
        readLock();
        try {
            return Lists.newArrayList(this.policyName, this.tokenizer, this.charFilterType, this.charFilterPattern,
                    this.charFilterReplacement, this.filterStopwords, this.filterLowercase);
        } finally {
            readUnlock();
        }
    }

    @Override
    public boolean matchPolicy(Policy checkedPolicyCondition) {
        if (!(checkedPolicyCondition instanceof InvertIndexPolicy)) {
            return false;
        }
        InvertIndexPolicy invertIndexPolicy = (InvertIndexPolicy) checkedPolicyCondition;
        return (invertIndexPolicy.getTokenizer() == null || invertIndexPolicy.getTokenizer().equals(this.tokenizer))
                && checkMatched(invertIndexPolicy.getType(), invertIndexPolicy.getPolicyName());
    }

    @Override
    public boolean matchPolicy(DropPolicyLog checkedDropCondition) {
        return checkMatched(checkedDropCondition.getType(), checkedDropCondition.getPolicyName());
    }

    @Override
    public boolean isInvalid() {
        return false;
    }

    @Override
    public void gsonPostProcess() throws IOException {
    }

    @Override
    public InvertIndexPolicy clone() {
        return new InvertIndexPolicy(this.id, this.policyName, this.tokenizer, this.charFilterType,
                this.charFilterPattern, this.charFilterReplacement, this.filterStopwords, this.filterLowercase);
    }
}
