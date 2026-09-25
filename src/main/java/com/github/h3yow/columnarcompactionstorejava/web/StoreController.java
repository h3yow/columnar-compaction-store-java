package com.github.h3yow.columnarcompactionstorejava.web;

import com.github.h3yow.columnarcompactionstorejava.store.model.CompactionResult;
import com.github.h3yow.columnarcompactionstorejava.store.model.ScanResult;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteBatch;
import com.github.h3yow.columnarcompactionstorejava.store.model.WriteResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 列式存储 HTTP 入口。批次被拒绝时仍以 200 返回带 accepted=false 与可区分原因的结果，
 * 便于调用方程序化区分（拒绝是业务结果而非服务错误）。
 */
@RestController
@RequestMapping("/api/store")
public class StoreController {

    private final StoreService service;

    public StoreController(StoreService service) {
        this.service = service;
    }

    @PostMapping("/write")
    public WriteResult write(@RequestBody WriteBatch batch) {
        return service.get().write(batch);
    }

    @GetMapping("/scan")
    public ScanResult scan() {
        return service.get().scan();
    }
}
