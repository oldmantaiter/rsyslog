/* omkafka.c
 * Use librdkafka to output to a kafka cluster.
 *
 * Copyright 2014 Tait Clarridge.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *       -or-
 *       see COPYING.ASL20 in the source distribution
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Author: Tait Clarridge <tait@clarridge.ca>
 *
 * Original Work (rsyslog v5): Babak Behzad <babakbehzad@gmail.com>
 *
 * TODO: Implement acks properly
 * TODO: Implement zookeeper configuration
 * TODO: Message callbacks to check whether we made it to Kafka (could be slow)
*/

#include "config.h"
#include "rsyslog.h"
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <signal.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>

#include "conf.h"
#include "syslogd-types.h"
#include "srUtils.h"
#include "template.h"
#include "module-template.h"
#include "cfsysline.h"
#include "errmsg.h"
#include "statsobj.h"
#include "unicode-helper.h"

#include <librdkafka/rdkafka.h>

MODULE_TYPE_OUTPUT
MODULE_TYPE_NOKEEP
MODULE_CNFNAME("omkafka")

DEF_OMOD_STATIC_DATA
DEFobjCurrIf(errmsg)
DEFobjCurrIf(statsobj)

statsobj_t *kafkaStats;
STATSCOUNTER_DEF(topicSubmit, mutCtrTopicSubmit)
STATSCOUNTER_DEF(kafkaFail, mutCtrKafkaFail)

typedef struct _instanceData {
    char *brokers;
    uchar *topic;
    sbool dynTopic;
    uchar *key;
    int partition;
    char *acks;
    char *maxBuffMsgs;
    char *maxRetries;
    char *retryBackoffMS;
    rd_kafka_t *rk;
    uchar *tplName;
} instanceData;

typedef struct wrkrInstanceData {
    instanceData *pData;
} wrkrInstanceData_t;

/* ----------------------------------------------------------------------------
 * Static definitions/initializations
 */

static struct cnfparamdescr actpdescr[] = {
    { "brokers", eCmdHdlrGetWord, 0 },
    { "topic", eCmdHdlrGetWord, 0 },
    { "key", eCmdHdlrGetWord, 0 },
    { "partition", eCmdHdlrInt, 0 },
    { "dyntopic", eCmdHdlrBinary, 0 },
    { "acks", eCmdHdlrGetWord, 0 },
    { "maxbuffmsgs", eCmdHdlrGetWord, 0 },
    { "maxretries", eCmdHdlrGetWord, 0 },
    { "retrybackoffms" , eCmdHdlrGetWord, 0 },
    { "template", eCmdHdlrGetWord, 0 },
};

static struct cnfparamblk actpblk =
    { CNFPARAMBLK_VERSION,
      sizeof(actpdescr)/sizeof(struct cnfparamdescr),
      actpdescr
    };

/* ----------------------------------------------------------------------------
 * Helper Functions
 */

/* Initialize the kafka objects and connect to the cluster */
static rsRetVal kafka_init(instanceData *pData) { 
    rd_kafka_conf_t *conf;
    DBGPRINTF("==> omkafka: kafka_init() begin...\n");
    DEFiRet;

    /* Ensure we have not already created the kafka object */
    ASSERT(pData != NULL);
    ASSERT(pData->rk == NULL);

    /* Producer config */
    conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "queue.buffering.max.ms", "0", NULL, 0);
    rd_kafka_conf_set(conf, "queue.buffering.max.messages", pData->maxBuffMsgs, NULL, 0);
    rd_kafka_conf_set(conf, "message.send.max.retries", pData->maxRetries, NULL, 0);
    rd_kafka_conf_set(conf, "retry.backoff.ms", pData->retryBackoffMS, NULL, 0);

    /* Setup the kafka producer object */
    if(!(pData->rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, NULL, 0))) {
        errmsg.LogError(0, RS_RET_SUSPENDED, 
                        "omkafka: kafka_init: rd_kafka_new failed\n");
        ABORT_FINALIZE(RS_RET_SUSPENDED);
    }

    /* Add the brokers to the kafka object
     * TODO: Add zookeeper support and test proper failover
    */
    if(rd_kafka_brokers_add(pData->rk, pData->brokers) < 1) {
        errmsg.LogError(0, RS_RET_SUSPENDED,
                        "omkafka: kafka_init: rd_kafka_brokers_add of '%s' failed\n", pData->brokers);
        ABORT_FINALIZE(RS_RET_SUSPENDED);
    }

    DBGPRINTF("==> omkafka: kafka_init() end...\n");
 finalize_it:
    RETiRet;
}

/* Cleanup and destroy */
static void kafka_close(instanceData *pData) { 
    DBGPRINTF("==> omkafka: kafka_close() begin...\n");
    ASSERT(pData != NULL);

    if(pData->rk != NULL)
        while (rd_kafka_outq_len(pData->rk) > 0) 
            /* gotta clear those queues */
            rd_kafka_poll(pData->rk, 1);
        rd_kafka_destroy(pData->rk);
    DBGPRINTF("==> omkafka: kafka_close() end...\n");
}

/* Send the message to the specified topic */
static rsRetVal kafka_sendmsg(instanceData *pData, uchar *msg, uchar *topic)
{
    int kmsgsize = 0;
    // rd_kafka_topic_conf_t *topic_conf;
    rd_kafka_topic_t *rkt = NULL;

    DEFiRet;
   
    DBGPRINTF("==> omkafka: kafka_sendmsg() begin...\n");

    /* String should not be NULL */
    ASSERT(msg != NULL);

    /* Connect to the kafka cluster if we have not already */
    if(pData->rk == NULL)
        CHKiRet(kafka_init(pData));

    /* TODO: Make this section work correctly, need to edit librdkafka so we NULL the
     * topic_conf object when passed into rd_kafka_topic_new. If we do not have that
     * this section will leak memory for topics that have been seen before.
        topic_conf = rd_kafka_topic_conf_new();
        rd_kafka_topic_conf_set(topic_conf, "request.required.acks", pData->acks, NULL, 0);
        rkt = rd_kafka_topic_new(pData->rk, (char*)topic, topic_conf);
        if (topic_conf != NULL)
            free(topic_conf);
    */

    /* Grabs a new rd_kafka_topic_t struct or if the topic already exists, an existing one.
     * This prevents the above-mentioned memory leaks or a double-free corruption.
    */
    rkt = rd_kafka_topic_new(pData->rk, (char*)topic, NULL);
    int keylen = pData->key ? strlen((char*)pData->key) : 0;

    /* Have librdkafka copy the msg object and free it internally, rsyslog needs
     * to be in control of the *msg object that was passed into this function
    */
    int sendflags = RD_KAFKA_MSG_F_COPY; 

    /* Strip newline from the message */
    kmsgsize = strlen((char*)msg);
    if (msg[kmsgsize-1] == '\n')
                msg[--kmsgsize] = '\0';

    DBGPRINTF("omkafka_sendmsg: ENTER - Syslogmessage = '%s' to Broker = '%s-%s'\n", (char*) msg, pData->brokers, topic);

    /* Finally we can send the message to the cluster */
    if(rd_kafka_produce(rkt, pData->partition, sendflags, msg, kmsgsize, pData->key, keylen, NULL) == -1) {
        DBGPRINTF("omkafka: Failed to produce to topic %s: %s\n", rd_kafka_topic_name(rkt), rd_kafka_err2str(rd_kafka_errno2err(errno)));
        /* Increment the failure counter and signal data failure*/
        STATSCOUNTER_INC(kafkaFail, mutCtrKafkaFail);
        ABORT_FINALIZE(RS_RET_DATAFAIL);

    }
    STATSCOUNTER_INC(topicSubmit, mutCtrTopicSubmit);
    /* For callback functions, which we do not have, but lets keep this in here */
    rd_kafka_poll(pData->rk, 0);

 finalize_it:
    DBGPRINTF("==> Sent %d bytes to topic %s partition %i\n", kmsgsize, rd_kafka_topic_name(rkt), pData->partition);
    DBGPRINTF("==> omkafka: kafka_sendmsg() end...\n");
    /* Destroy every time */
    rd_kafka_topic_destroy(rkt);
    RETiRet;
}

/* Set defaults for our instanceData struct */
static inline void
setInstParamDefaults(instanceData* pData) {
    pData->tplName = NULL;
    pData->brokers = NULL;
    pData->topic = NULL;
    pData->dynTopic = 0;
    pData->key = NULL;
    /* No partition by default */
    pData->partition = RD_KAFKA_PARTITION_UA;
    pData->acks = NULL;
    pData->maxBuffMsgs = NULL;
    pData->maxRetries = NULL;
    pData->retryBackoffMS = NULL;
}


/* ----------------------------------------------------------------------------
 * Output Module Functions
 */


BEGINcreateInstance
CODESTARTcreateInstance
ENDcreateInstance

BEGINcreateWrkrInstance
CODESTARTcreateWrkrInstance
ENDcreateWrkrInstance

BEGINisCompatibleWithFeature
CODESTARTisCompatibleWithFeature
    if(eFeat == sFEATURERepeatedMsgReduction)
        iRet = RS_RET_OK;
ENDisCompatibleWithFeature

BEGINdbgPrintInstInfo
CODESTARTdbgPrintInstInfo
    DBGPRINTF("brokers: %s\n", pData->brokers);
    DBGPRINTF("topic: %s\n", pData->topic);
    DBGPRINTF("key: %s\n", pData->key);
    DBGPRINTF("parition: %d\n", pData->partition);
    DBGPRINTF("acks: %s\n", pData->acks);
ENDdbgPrintInstInfo


BEGINfreeInstance
CODESTARTfreeInstance
    kafka_close(pData);
    free(pData->brokers);
    free(pData->tplName);
    free(pData->key);
    free(pData->topic);
    free(pData->acks);
    free(pData->maxBuffMsgs);
    free(pData->maxRetries);
    free(pData->retryBackoffMS);
ENDfreeInstance

BEGINfreeWrkrInstance
CODESTARTfreeWrkrInstance
ENDfreeWrkrInstance

BEGINtryResume
CODESTARTtryResume
    if(pWrkrData->pData->rk == NULL)
        iRet = kafka_init(pWrkrData->pData);
ENDtryResume


BEGINdoAction
instanceData *pData = pWrkrData->pData;
CODESTARTdoAction
    if(pData->dynTopic)
        iRet = kafka_sendmsg(pData, ppString[0], ppString[1]);
    else
        iRet = kafka_sendmsg(pData, ppString[0], pData->topic);
ENDdoAction

BEGINnewActInst
    struct cnfparamvals *pvals;
    int i;
    int iNumTpls;
CODESTARTnewActInst
    if ((pvals = nvlstGetParams(lst, &actpblk, NULL)) == NULL) {
        ABORT_FINALIZE(RS_RET_MISSING_CNFPARAMS);
    }

    CHKiRet(createInstance(&pData));
    setInstParamDefaults(pData);

    for (i = 0; i < actpblk.nParams; ++i) {
        if (!pvals[i].bUsed)
            continue;
        if (!strcmp(actpblk.descr[i].name, "brokers")) {
            pData->brokers = es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "template")) {
            pData->tplName = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "key")){
            pData->key = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "topic")){
            pData->topic = (uchar*)es_str2cstr(pvals[i].val.d.estr, NULL);
        } else if (!strcmp(actpblk.descr[i].name, "partition")) {
            pData->partition = (int) pvals[i].val.d.n;
        } else if(!strcmp(actpblk.descr[i].name, "dyntopic")) {
            pData->dynTopic = pvals[i].val.d.n;
        } else if (!strcmp(actpblk.descr[i].name, "acks")){
            pData->acks = es_str2cstr(pvals[i].val.d.estr, "0");
        } else if (!strcmp(actpblk.descr[i].name, "maxbuffmsgs")){
            pData->maxBuffMsgs = es_str2cstr(pvals[i].val.d.estr, "500000");
        } else if (!strcmp(actpblk.descr[i].name, "maxretries")){
            pData->maxRetries = es_str2cstr(pvals[i].val.d.estr, "3");
        } else if (!strcmp(actpblk.descr[i].name, "retrybackoffms")){
            pData->retryBackoffMS = es_str2cstr(pvals[i].val.d.estr, "500");
        } else {
            errmsg.LogError(0, NO_ERRCODE, "omkafka: program error, non-handled "
                            "param '%s'\n", actpblk.descr[i].name);
        }
    }

    if (pData->brokers == NULL) {
        errmsg.LogError(0, RS_RET_CONFIG_ERROR, "omkafka: you didn't enter a broker");
        ABORT_FINALIZE(RS_RET_CONFIG_ERROR);
    }
    if (pData->dynTopic && pData->topic == NULL) {
        errmsg.LogError(0, RS_RET_CONFIG_ERROR,
            "omkafka: requested dynamic topic, but no "
            "name for topic template given - action definition invalid");
        ABORT_FINALIZE(RS_RET_CONFIG_ERROR);
    }
    if (pData->topic == NULL) {
        errmsg.LogError(0, RS_RET_CONFIG_ERROR, "omkafka: you didn't enter a topic");
        ABORT_FINALIZE(RS_RET_CONFIG_ERROR);
    }

    /* Allow templating for the topic and msg format */
    iNumTpls = 1;
    if(pData->dynTopic) ++iNumTpls;
    DBGPRINTF("omkafka: requesting %d templates\n", iNumTpls);
    CODE_STD_STRING_REQUESTnewActInst(iNumTpls);
    if (pData->tplName == NULL) {
        CHKiRet(OMSRsetEntry(*ppOMSR, 0, (uchar*)strdup("RSYSLOG_FileFormat"), OMSR_NO_RQD_TPL_OPTS));
    } else {
        CHKiRet(OMSRsetEntry(*ppOMSR, 0, (uchar*)strdup((char*)pData->tplName), OMSR_NO_RQD_TPL_OPTS));
    }
    if(pData->dynTopic) {
        CHKiRet(OMSRsetEntry(*ppOMSR, 1, ustrdup(pData->topic),
            OMSR_NO_RQD_TPL_OPTS));
    }

CODE_STD_FINALIZERnewActInst
    cnfparamvalsDestruct(pvals, &actpblk);
ENDnewActInst

BEGINparseSelectorAct
CODESTARTparseSelectorAct
/* We should only use the new v6 config */
CODE_STD_STRING_REQUESTparseSelectorAct(1)
    if(!strncmp((char*) p, ":omkafka:", sizeof(":omkafka:") - 1)) 
        errmsg.LogError(0, RS_RET_LEGA_ACT_NOT_SUPPORTED,
            "omkafka supports only v6 config format, use: "
            "action(type=\"omkafka\" brokers=...)");
    ABORT_FINALIZE(RS_RET_CONFLINE_UNPROCESSED);
CODE_STD_FINALIZERparseSelectorAct
ENDparseSelectorAct


/* mode exit */
BEGINmodExit
CODESTARTmodExit
    statsobj.Destruct(&kafkaStats);
    objRelease(errmsg, CORE_COMPONENT);
    objRelease(statsobj, CORE_COMPONENT);
ENDmodExit


BEGINqueryEtryPt
CODESTARTqueryEtryPt
CODEqueryEtryPt_STD_OMOD_QUERIES
CODEqueryEtryPt_STD_OMOD8_QUERIES
CODEqueryEtryPt_STD_CONF2_OMOD_QUERIES
ENDqueryEtryPt


BEGINmodInit()
CODESTARTmodInit
    *ipIFVersProvided = CURR_MOD_IF_VERSION; /* we only support the current interface specification */
CODEmodInit_QueryRegCFSLineHdlr
    CHKiRet(objUse(errmsg, CORE_COMPONENT));
    CHKiRet(objUse(statsobj, CORE_COMPONENT));
    DBGPRINTF("omkafka: module compiled with rsyslog version %s.\n", VERSION);
    CHKiRet(statsobj.Construct(&kafkaStats));
    CHKiRet(statsobj.SetName(kafkaStats, (uchar *)"omkafka"));
    STATSCOUNTER_INIT(topicSubmit, mutCtrTopicSubmit);
    CHKiRet(statsobj.AddCounter(kafkaStats, (uchar *)"submitted",
        ctrType_IntCtr, CTR_FLAG_RESETTABLE, &topicSubmit));
    STATSCOUNTER_INIT(kafkaFail, mutCtrKafkaFail);
    CHKiRet(statsobj.AddCounter(kafkaStats, (uchar *)"failures",
        ctrType_IntCtr, CTR_FLAG_RESETTABLE, &kafkaFail));
    CHKiRet(statsobj.ConstructFinalize(kafkaStats));

ENDmodInit

