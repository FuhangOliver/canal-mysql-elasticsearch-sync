package com.ciwei.canal.controller;

import com.ciwei.App;
import com.ciwei.canal.dao.BaseDao;
import com.ciwei.canal.model.request.SyncByTableRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

/**
 * @author fuhang
 * @description: mysql数据同步到es中
 * @date 2020/9/28 16:14
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = {App.class})
public class SyncControllerTest {

    @Autowired
    private SyncController syncController;

    @Resource
    private BaseDao baseDao;

    @Test
    public void syncTable() {
        SyncByTableRequest syncByTableRequest = new SyncByTableRequest();
        syncByTableRequest.setDatabase("ald_recruit");
        syncByTableRequest.setTable("rsm_personal_information");
        syncController.syncTable(syncByTableRequest);
    }

    @Test
    public void selectByPK() {
        System.out.println(baseDao.selectByPK("id","1732","ald_recruit","rsm_personal_information"));
    }

    @Test
    public void selectFieldValueDataType() {
        System.out.println(baseDao.selectFieldValueDataType("ald_recruit","rsm_personal_information"));
    }
}