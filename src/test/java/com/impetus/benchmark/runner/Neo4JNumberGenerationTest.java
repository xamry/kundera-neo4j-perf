/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.benchmark.runner;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Kuldeep Mishra
 * 
 */
public class Neo4JNumberGenerationTest
{

    private static String propertyFileName;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        propertyFileName = System.getProperty("fileName");
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
    }
    
     /**
     * Test method for
     * {@link com.impetus.benchmark.runner.Neo4JNumberGeneration#main(java.lang.String[])}
     * .
     */
    @Test
    public void testMongo()
    {
        try
        {
            Neo4JNumberGeneration.main(new String[] { propertyFileName });
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
